import os
import sys
import tempfile
from datetime import datetime, timezone
from functools import partial

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json
from pymongo import MongoClient, UpdateOne

from processing.gameMoves import analyze_chess_game
from processing.schemas import PAYLOAD_SCHEMA, envelope_schema
from ingestion.common import TOPIC_GAMES, mongo_db

# Kafka, MongoDB and checkpoint settings
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DATABASE = "chess_db"
COLLECTION = "chess_move_evals"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
CHECKPOINT = "/tmp/chess-eval-checkpoint"

# Stockfish search depth, stored on every document so different depths stay distinguishable
DEPTH = int(os.getenv("STOCKFISH_DEPTH", 16))

# Games per micro-batch, Spark partitions per batch and documents per MongoDB bulk write
GAMES_PER_BATCH = int(os.getenv("EVAL_GAMES_PER_BATCH", 20))
EVAL_PARTITIONS = int(os.getenv("EVAL_PARTITIONS", 8))
MONGO_BATCH = int(os.getenv("EVAL_MONGO_BATCH", 500))


# Spark session for the evaluation job
def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("chess-stockfish-evaluator")
        .config("spark.log.level", "WARN")
    )

    # Forward STOCKFISH_PATH to the executors, which are the ones running the engine
    if os.getenv("STOCKFISH_PATH"):
        builder = builder.config("spark.executorEnv.STOCKFISH_PATH", os.environ["STOCKFISH_PATH"])

    return builder.getOrCreate()


# Streams games.raw, keeping only the fields the engine and its results need
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
        .select(
            col("uuid").alias("game_id"),
            col("pgn"),
            col("white.username").alias("white_username"),
            col("black.username").alias("black_username"),
        )
    )


# Runs on an executor: evaluates each game in the partition and bulk-writes the ranks from there
def evaluate_partition(
    rows,
    depth: int,
    engine_id: str,
    mongo_uri: str,
    database: str,
    collection_name: str,
    mongo_batch: int,
) -> None:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000)

    # Per-task scratch file, since analyze_chess_game also writes its result to disk
    handle, scratch_path = tempfile.mkstemp(prefix="move_ranks_", suffix=".json")
    os.close(handle)

    try:
        collection = client[database][collection_name]
        evaluated_at = datetime.now(timezone.utc).isoformat()
        pending: list[UpdateOne] = []

        for row in rows:
            # Skip games already evaluated at this engine setting
            if collection.find_one({"game_id": row.game_id, "engine": engine_id}, {"_id": 1}):
                continue

            # An unreadable PGN skips the game rather than failing the batch
            try:
                move_ranks = analyze_chess_game(row.pgn, output_path=scratch_path, depth=depth)
            except ValueError:
                continue

            pending.extend(build_operations(row, move_ranks, engine_id, evaluated_at))

            # Flush in chunks so memory stays flat regardless of partition size
            if len(pending) >= mongo_batch:
                flush(collection, pending)

        flush(collection, pending)
    finally:
        os.unlink(scratch_path)
        client.close()


# One upsert per ply onto the document processor.py writes for the same ply, so the two merge
def build_operations(row, move_ranks: dict, engine_id: str, evaluated_at: str):
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


# Unordered bulk write, so one rejected document does not abandon the rest
def flush(collection, pending: list[UpdateOne]) -> None:
    if not pending:
        return

    collection.bulk_write(pending, ordered=False)
    pending.clear()


# Deduplicates the batch, spreads its games across executors and evaluates them there
def process_micro_batch(batch_df: DataFrame, batch_id: int, worker) -> None:
    (
        batch_df
        .dropDuplicates(["game_id"])
        .repartition(EVAL_PARTITIONS)
        .rdd
        .foreachPartition(worker)
    )


# Indexes backing the skip check and per-player rank queries
def ensure_indexes() -> None:
    collection = mongo_db(DATABASE)[COLLECTION]
    collection.create_index([("game_id", 1), ("engine", 1)], name="game_engine")
    collection.create_index([("username", 1), ("rank", 1)], name="player_rank")


# Evaluation stream with its own checkpoint, independent of processor.py
def evaluate_game_data(spark: SparkSession, once: bool = False) -> StreamingQuery:
    games_df = read_games(spark)

    engine_id = f"stockfish-d{DEPTH}"

    # Bind the settings for the executors, which receive only each partition's rows
    worker = partial(
        evaluate_partition,
        depth=DEPTH,
        engine_id=engine_id,
        mongo_uri=MONGO_URI,
        database=DATABASE,
        collection_name=COLLECTION,
        mongo_batch=MONGO_BATCH,
    )

    writer = (
        games_df.writeStream
        .foreachBatch(partial(process_micro_batch, worker=worker))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
    )

    # --once drains the current backlog and then stops
    if once:
        writer = writer.trigger(availableNow=True)

    return writer.start()


# Creates the indexes, then runs the evaluation stream until it terminates
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
