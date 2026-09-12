import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from functools import partial

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json
from pymongo import MongoClient, UpdateOne

from processing.gameMoves import analyze_chess_game
from processing.schemas import PAYLOAD_SCHEMA, envelope_schema
from ingestion.common import TOPIC_GAMES, mongo_db

# To run use 'docker compose exec python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --master spark://spark-master:7077 /CS4010/processing/evaluator.py'
# Add --once to drain whatever is on the topic and exit, instead of staying up for new games.

# This is a SECOND job, submitted separately from processor.py, and that is the
# whole point. Stockfish is thousands of times slower than the PGN parsing in
# processor.py, so the two cannot share a query: one slow engine search would
# hold up every cheap move row behind it. Kafka fans games.raw out to both jobs
# (Structured Streaming tracks its own offsets per checkpoint, so there is no
# shared consumer group to fight over), which means:
#   - either job can be restarted, redeployed or scaled without touching the other
#   - the evaluator can be run on a bigger cluster than the parser, because it is
#     the only CPU-bound part of the pipeline
#   - a crash in the engine does not lose the parsed game data

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DATABASE = "chess_db"
COLLECTION = "chess_move_evals"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
CHECKPOINT = "/tmp/chess-eval-checkpoint"

# Search depth per position. gameMoves searches twice per ply (once before the
# move, once after), so a 59-ply average game is ~119 full-depth searches and the
# cost grows about 1.6x per ply of depth. Measured on Stockfish 14.1, scaled to
# that average game: depth 10 = 4.8s, 12 = 14.7s, 14 = 34.4s, 17 = 142.1s.
# 16 is the chosen operating point. The value is recorded on every document, so
# evaluations made at different settings stay tellable apart -- and the skip
# check below keys on it, so changing it re-evaluates rather than silently
# mixing depths in one collection.
DEPTH = int(os.getenv("STOCKFISH_DEPTH", 16))

# Backpressure. Without a cap the first trigger pulls every game on the topic
# into one micro-batch, and a batch that takes hours reports no progress and
# checkpoints nothing. Small batches keep the job restartable.
GAMES_PER_BATCH = int(os.getenv("EVAL_GAMES_PER_BATCH", 20))

# Games arrive on 3 Kafka partitions but differ wildly in length, so the batch is
# reshuffled across more tasks than there are cores. Short games free their slot
# early instead of idling behind a 200-ply game in the same partition.
EVAL_PARTITIONS = int(os.getenv("EVAL_PARTITIONS", 8))

# Move documents per bulk_write. One round trip per ply would spend more time in
# the network than in the engine.
MONGO_BATCH = int(os.getenv("EVAL_MONGO_BATCH", 500))


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("chess-stockfish-evaluator")  # own app name, so it is a separate entry in the Spark UI
        .config("spark.log.level", "WARN")
    )

    # gameMoves resolves STOCKFISH_PATH at import time, and it is the executors
    # that import it, so the setting has to reach them. Only forwarded when it is
    # set explicitly -- otherwise the executors resolve the binary themselves and
    # a driver image with a different layout cannot mislead them.
    if os.getenv("STOCKFISH_PATH"):
        builder = builder.config("spark.executorEnv.STOCKFISH_PATH", os.environ["STOCKFISH_PATH"])

    return builder.getOrCreate()


def read_games(spark: SparkSession) -> DataFrame:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC_GAMES)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", GAMES_PER_BATCH)
        .load()
    )

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), envelope_schema(PAYLOAD_SCHEMA)).alias("message")
    ).select("message.payload.*")

    return (
        parsed_df
        .filter(col("uuid").isNotNull())
        .filter(col("pgn").isNotNull())
        # There is no variant or time-class filter here on purpose:
        # collect_games.py drops everything that is not standard blitz, rapid or
        # bullet before it reaches the topic, so the engine never sees a position
        # it would read by the wrong rules.
        #
        # Projection before the shuffle: only what the engine needs plus what
        # identifies the result. Everything else about the game is already in
        # chess_games, written by processor.py, and joinable on _id.
        .select(
            col("uuid").alias("game_id"),
            col("pgn"),
            col("white.username").alias("white_username"),
            col("black.username").alias("black_username"),
        )
    )


def evaluate_partition(
    rows,
    depth: int,
    engine_id: str,
    mongo_uri: str,
    database: str,
    collection_name: str,
    mongo_batch: int,
    counters,
) -> None:
    """Analyse one partition's games on an executor and write them from there.

    Deliberately not routed through the driver the way processor.py's
    write_to_mongodb is. That helper calls toLocalIterator(), which is fine for
    the parser, but this job produces a document per ply and spends minutes of
    CPU per task -- pulling that back to a single driver would undo the
    parallelism the job exists for. So: one Mongo connection per partition, and
    the writes leave from the executor that did the work.

    analyze_chess_game starts and stops its own Stockfish process per game. That
    costs a process spawn per game rather than per partition, which is noise next
    to a search that runs for seconds per move, and it keeps the analyzer usable
    on its own. If the engine ever becomes the startup bottleneck, hoisting the
    popen_uci call out of gameMoves is the change to make.
    """
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000)

    # analyze_chess_game writes its ranks to a file as well as returning them.
    # One scratch path per task, reused and removed, keeps concurrent executors
    # from writing over each other and keeps the container filesystem clean.
    handle, scratch_path = tempfile.mkstemp(prefix="move_ranks_", suffix=".json")
    os.close(handle)

    try:
        collection = client[database][collection_name]
        evaluated_at = datetime.now(timezone.utc).isoformat()
        pending: list[UpdateOne] = []

        for row in rows:
            # Idempotence, cheaply. The checkpoint already stops a clean restart
            # from redoing work, but a replayed offset range or a dropped
            # checkpoint would otherwise re-run an hour of engine time. One
            # indexed lookup costs milliseconds against a game that costs
            # minutes. Keyed on engine_id too, so changing the depth
            # re-evaluates instead of silently keeping the old numbers.
            if collection.find_one({"game_id": row.game_id, "engine": engine_id}, {"_id": 1}):
                counters["skipped"] += 1
                continue

            try:
                move_ranks = analyze_chess_game(row.pgn, output_path=scratch_path, depth=depth)
            except ValueError:  # unparseable movetext, one bad game must not fail the batch
                counters["failed"] += 1
                continue

            operations = list(build_operations(row, move_ranks, engine_id, evaluated_at))
            pending.extend(operations)

            # Accumulators, so the driver can report progress: an executor's own
            # print goes to the worker's stderr where nobody is looking.
            counters["games"] += 1
            counters["docs"] += len(operations)

            # Flush on a document count rather than at the end of the partition,
            # so memory stays flat no matter how many games land in one task.
            if len(pending) >= mongo_batch:
                flush(collection, pending)

        flush(collection, pending)
    finally:
        os.unlink(scratch_path)
        client.close()


def build_operations(row, move_ranks: dict, engine_id: str, evaluated_at: str):
    """One upsert per ply, onto the document processor.py writes for the same ply.

    Both jobs key on {game_id}_ply_{ply} and both $set, so the move's own facts
    (SAN, clock, time spent, position) and its rank merge into one document
    whichever job gets there first -- and neither has to wait for the other.

    This job writes only what evaluation produces. The one thing copied across is
    the username: 'blunder rate per player' is the question this collection exists
    to answer, and it should not need a join to chess_games to ask it.
    """
    usernames = {"White": row.white_username, "Black": row.black_username}

    for ply, ranking in move_ranks.items():
        yield UpdateOne(
            {"_id": f"{row.game_id}_ply_{ply}"},
            {
                "$set": {
                    "game_id": row.game_id,
                    "ply": int(ply),
                    "username": usernames.get(ranking["side"]),
                    "rank": ranking["rank"],
                    "engine": engine_id,
                    "evaluated_at": evaluated_at,
                }
            },
            upsert=True,
        )


def flush(collection, pending: list[UpdateOne]) -> None:
    if not pending:
        return

    # ordered=False so the writes go out in parallel and one rejected document
    # does not abandon the rest of the batch.
    collection.bulk_write(pending, ordered=False)
    pending.clear()


def log(message: str) -> None:
    """Timestamped line on the driver. Executor prints land in the worker's own
    stderr, which nobody watching the submit ever sees, so anything meant to
    prove the job is alive has to be printed here."""
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {message}", flush=True)


def process_micro_batch(batch_df: DataFrame, batch_id: int, worker, counters) -> None:
    started = time.perf_counter()
    before = {name: acc.value for name, acc in counters.items()}

    work = (
        batch_df
        # Archives overlap, so the same game can appear twice on the topic.
        # Catching it here saves the engine run; the upsert below would only
        # save the duplicate document.
        .dropDuplicates(["game_id"])
        .repartition(EVAL_PARTITIONS)
    )

    # Logged BEFORE the work, not only after: a batch of GAMES_PER_BATCH games at
    # depth 16 runs for many minutes with nothing to show, and "started" is what
    # tells you the difference between slow and stuck.
    log(f"batch {batch_id:>4} | starting, up to {GAMES_PER_BATCH} games on {EVAL_PARTITIONS} partitions")

    work.rdd.foreachPartition(worker)

    elapsed = time.perf_counter() - started
    done = counters["games"].value - before["games"]
    skipped = counters["skipped"].value - before["skipped"]
    failed = counters["failed"].value - before["failed"]
    docs = counters["docs"].value - before["docs"]
    rate = done / elapsed * 60 if elapsed > 0 else 0.0

    log(
        f"batch {batch_id:>4} | {done:>3} games in {elapsed/60:>5.1f} min "
        f"({rate:>5.2f} games/min) | {docs:>5} move evals"
        f"{f' | {skipped} already done' if skipped else ''}"
        f"{f' | {failed} unreadable' if failed else ''}"
        f" || TOTAL {counters['games'].value:,} games, {counters['docs'].value:,} evals"
    )


def ensure_indexes() -> None:
    """Driver-side, once at start-up. The skip check above is only cheap if the
    lookup is indexed, and an unindexed collection scan per game would grow into
    the job's bottleneck as the collection fills."""
    collection = mongo_db(DATABASE)[COLLECTION]
    collection.create_index([("game_id", 1), ("engine", 1)], name="game_engine")
    collection.create_index([("username", 1), ("rank", 1)], name="player_rank")
    # Exactly one document per game has ply 1, so this turns "how many games are
    # evaluated" into an index count instead of a distinct() over every move row.
    # status.py leans on it; at millions of moves the difference is seconds vs minutes.
    collection.create_index([("ply", 1), ("engine", 1)], name="ply_engine")


def evaluate_game_data(spark: SparkSession, once: bool = False) -> StreamingQuery:
    games_df = read_games(spark)

    engine_id = f"stockfish-d{DEPTH}"

    counters = {
        name: spark.sparkContext.accumulator(0)
        for name in ("games", "docs", "skipped", "failed")
    }

    # partial because foreachPartition hands the function nothing but the rows,
    # and the executor has no access to this module's constants otherwise.
    worker = partial(
        evaluate_partition,
        depth=DEPTH,
        engine_id=engine_id,
        mongo_uri=MONGO_URI,
        database=DATABASE,
        collection_name=COLLECTION,
        mongo_batch=MONGO_BATCH,
        counters=counters,
    )

    log(f"evaluator up | engine {engine_id} | {EVAL_PARTITIONS} partitions | {GAMES_PER_BATCH} games/batch | -> {DATABASE}.{COLLECTION}")

    writer = (
        games_df.writeStream
        .foreachBatch(partial(process_micro_batch, worker=worker, counters=counters))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)  # its own checkpoint, so its offsets are independent of processor.py's
    )

    if once:
        # Same code path as a catch-up batch job: work through the backlog in
        # GAMES_PER_BATCH-sized batches, commit each one, then stop.
        writer = writer.trigger(availableNow=True)

    return writer.start()


def main() -> None:
    ensure_indexes()

    spark = create_spark_session()
    query = evaluate_game_data(spark, once="--once" in sys.argv)

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Evaluation stream failed {e}")

        if query.isActive:
            query.stop()


if __name__ == "__main__":
    main()
