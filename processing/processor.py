import os
import io
import re
import chess.pgn
from functools import partial
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, concat_ws, from_json, coalesce, lit, explode, udf, from_unixtime, lower, date_format
from processing.schemas import PLAYER_SCHEMA, PAYLOAD_SCHEMA, PGN_GAME_SCHEMA, envelope_schema
from ingestion.common import TOPIC_PLAYERS, TOPIC_GAMES, mongo_db
from pymongo import UpdateOne

# Kafka and MongoDB connection settings
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DATABASE = "chess_db"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

# Kafka messages per micro-batch; games get a lower cap as each explodes into ~60 move rows
PLAYERS_PER_TRIGGER = int(os.getenv("PLAYERS_PER_TRIGGER", 5000))
GAMES_PER_TRIGGER = int(os.getenv("GAMES_PER_TRIGGER", 500))


# Spark session for the parsing job
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("chess-streaming-processor")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )
    return spark

# Streaming read of one Kafka topic, capped per micro-batch to bound driver memory
def read_stream(spark: SparkSession, const_topic, max_per_trigger: int) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", const_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_per_trigger)
        .load()
    )

# Upserts every row of a micro-batch into a MongoDB collection, keyed on the id_path field
def write_to_mongodb(batch_df: DataFrame, batch_id: int, collection_name: str, id_path: tuple[str, ...]) -> None:
    operations = []

    # One upsert per row, streamed to the driver and skipping rows without an id
    for row in batch_df.toLocalIterator():
        document = row.asDict(recursive=True)
        document_id = document
        for field in id_path:
            if not isinstance(document_id, dict):
                document_id = None
                break
            document_id = document_id.get(field)

        if document_id is None:
            continue

        operations.append(
            UpdateOne(
                {"_id": document_id},
                {"$set": document},
                upsert=True
            )
        )

    if not operations:
        return

    # Single unordered bulk write per batch
    db = mongo_db(DATABASE)
    collection = db[collection_name]
    collection.bulk_write(operations, ordered=False)

# Parses Kafka JSON into columns, unwrapping the collector envelope when present
def parse_df(kafka_df: DataFrame, schema: StructType, enveloped: bool = True) -> DataFrame:
    json_column = col("value").cast("string")

    if enveloped:
        parsed = kafka_df.select(
            from_json(json_column, envelope_schema(schema)).alias("message"),
        )
        return parsed.select("message.payload.*", "message.fetched_at")

    return kafka_df.select(from_json(json_column, schema).alias("data")).select("data.*")

# Streams players.raw into chess_players as one flat document per player
def process_player_data(spark: SparkSession) -> StreamingQuery:
    kafka_df = read_stream(spark, TOPIC_PLAYERS, PLAYERS_PER_TRIGGER)

    parsed_df = parse_df(kafka_df, PLAYER_SCHEMA)

    # Drop messages missing stats or identity
    processed_df = (
        parsed_df
        .filter(col("stats").isNotNull())
        .filter(col("username").isNotNull())
        .filter(col("country_hint").isNotNull())
    )

    # Games played per time class and in total
    mode_totals = {}
    for chess_gamemode in ("blitz", "rapid", "bullet"):
        record = col(f"stats.chess_{chess_gamemode}.record")
        mode_totals[chess_gamemode] = (
            coalesce(record.getField("win"), lit(0))
            + coalesce(record.getField("loss"), lit(0))
            + coalesce(record.getField("draw"), lit(0))
        )

    for chess_gamemode, total in mode_totals.items():
        processed_df = processed_df.withColumn(
            f"total_games_played_{chess_gamemode}", total.cast("long")
        )

    processed_df = processed_df.withColumn(
        "total_games_played",
        sum(mode_totals.values()).cast("long")
    )

    # Flatten to one level for Power BI, keyed on the lowercased username
    flattened_df = processed_df.select(
        lower(col("username")).alias("_id"),
        col("username"),
        col("country_hint").alias("country_code"),
        col("fetched_at"),

        col("total_games_played"),
        col("total_games_played_blitz"),
        col("total_games_played_rapid"),
        col("total_games_played_bullet"),

        col("stats.chess_blitz.last.rating").alias("blitz_rating_current"),
        col("stats.chess_blitz.best.rating").alias("blitz_rating_best"),
        date_format(from_unixtime(col("stats.chess_blitz.best.date")), "yyyy-MM-dd").alias("blitz_peak_date"),
        col("stats.chess_blitz.record.win").alias("blitz_wins"),
        col("stats.chess_blitz.record.loss").alias("blitz_losses"),
        col("stats.chess_blitz.record.draw").alias("blitz_draws"),

        col("stats.chess_rapid.last.rating").alias("rapid_rating_current"),
        col("stats.chess_rapid.best.rating").alias("rapid_rating_best"),
        date_format(from_unixtime(col("stats.chess_rapid.best.date")), "yyyy-MM-dd").alias("rapid_peak_date"),
        col("stats.chess_rapid.record.win").alias("rapid_wins"),
        col("stats.chess_rapid.record.loss").alias("rapid_losses"),
        col("stats.chess_rapid.record.draw").alias("rapid_draws"),

        col("stats.chess_bullet.last.rating").alias("bullet_rating_current"),
        col("stats.chess_bullet.best.rating").alias("bullet_rating_best"),
        date_format(from_unixtime(col("stats.chess_bullet.best.date")), "yyyy-MM-dd").alias("bullet_peak_date"),
        col("stats.chess_bullet.record.win").alias("bullet_wins"),
        col("stats.chess_bullet.record.loss").alias("bullet_losses"),
        col("stats.chess_bullet.record.draw").alias("bullet_draws")
    )

    # Upsert into chess_players keyed on _id, with its own checkpoint
    player_writer = partial(
        write_to_mongodb,
        collection_name="chess_players",
        id_path=("_id",)
    )

    return (
        flattened_df.writeStream
        .foreachBatch(player_writer)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/chess-player-checkpoint")
        .start()
    )

# Splits a PGN TimeControl header such as "180+2" into base and increment seconds
def parse_time_control(tc_str):
    base, _, increment = (tc_str or "").partition("+")

    # Non-numeric controls such as daily or unlimited fall back to 180 seconds
    try:
        base_sec = float(base)
    except ValueError:
        base_sec = 180.0

    try:
        increment_sec = float(increment) if increment else 0.0
    except ValueError:
        increment_sec = 0.0

    return base_sec, increment_sec


# Converts a clock annotation in h:mm:ss, mm:ss or ss form to seconds
def clock_to_seconds(clk_str):
    if not clk_str:
        return None
    parts = clk_str.split(":")
    if len(parts) == 3:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return float(parts[0]) * 60 + float(parts[1])
    return float(parts[0])

# UDF parsing a PGN into its headers and per-move SAN, clock, time spent and position
@udf(returnType=PGN_GAME_SCHEMA)
def parse_df_pgn(raw_pgn_str):
    if not raw_pgn_str:
        return None

    # An unparseable PGN yields no result
    pgn_io = io.StringIO(raw_pgn_str)
    game = chess.pgn.read_game(pgn_io)
    if game is None:
        return None

    # Clock annotations look like {[%clk 0:02:59.9]}
    clk = re.compile(r"\[%clk\s+([\d:\.]+)\]")

    headers = game.headers

    # Base time and increment are both needed to recover the time spent per move
    tc_str = headers.get("TimeControl", "180")
    base_tc, increment = parse_time_control(tc_str)

    parsed_moves = []
    board = game.board()
    prev_clocks = {"white": base_tc, "black": base_tc}

    # Replay the game move by move, recording each position and clock
    for node in game.mainline():
        ply = node.ply()
        move_num = (ply + 1) // 2
        fen_before = board.fen()

        # Side to move and the move in standard algebraic notation
        player = "white" if board.turn == chess.WHITE else "black"
        move_san = board.san(node.move)

        # Clock after the move, left empty when the PGN has no annotation
        match = clk.search(node.comment)
        remaining_sec = None
        time_spent_sec = None

        # Time spent is the previous clock plus the increment, minus the clock after the move
        if match:
            remaining_sec = clock_to_seconds(match.group(1))
            if prev_clocks[player] is not None:
                time_spent_sec = max(0.0, round(prev_clocks[player] + increment - remaining_sec, 2))
            prev_clocks[player] = remaining_sec

        parsed_moves.append({
            "move_san": move_san,
            "remaining_clock_sec": remaining_sec,
            "ply": ply,
            "move_number": move_num,
            "player": player,
            "time_spent_sec": time_spent_sec,
            "fen_before": fen_before
        })

        board.push(node.move)

    return {
        "termination": headers.get("Termination"),
        "eco": headers.get("ECO"),
        "moves": parsed_moves
    }

# Writes each game at two grains: one chess_games document, and one chess_move_evals document per ply
def write_game_data(batch_df: DataFrame, batch_id: int) -> None:
    # Game-level facts only
    games_df = batch_df.select(
        col("uuid").alias("_id"),
        col("rated"),
        col("time_control"),
        col("time_class"),
        col("fen").alias("final_fen"),

        col("parsed_pgn.eco").alias("eco"),
        col("parsed_pgn.termination").alias("termination"),

        col("white.username").alias("white_username"),
        col("white.rating").alias("white_rating"),
        col("white.result").alias("white_result"),
        col("black.username").alias("black_username"),
        col("black.rating").alias("black_rating"),
        col("black.result").alias("black_result"),
    )

    # One row per move, keyed like evaluator.py so its rank merges onto the same document
    moves_df = (
        batch_df
        .select(
            col("uuid").alias("game_id"),
            explode(col("parsed_pgn.moves")).alias("move"),
        )
        .select(
            concat_ws("_ply_", col("game_id"), col("move.ply").cast("string")).alias("_id"),
            col("game_id"),
            col("move.ply").alias("ply"),
            col("move.move_number").alias("move_number"),
            col("move.player").alias("player"),
            col("move.move_san").alias("move_san"),
            col("move.remaining_clock_sec").alias("remaining_clock_sec"),
            col("move.time_spent_sec").alias("time_spent_sec"),
            col("move.fen_before").alias("fen_before"),
        )
    )

    write_to_mongodb(games_df, batch_id, "chess_games", ("_id",))
    write_to_mongodb(moves_df, batch_id, "chess_move_evals", ("_id",))


# Streams games.raw through the PGN UDF into chess_games and chess_move_evals
def process_game_data(spark: SparkSession) -> StreamingQuery:
    kafka_df = read_stream(spark, TOPIC_GAMES, GAMES_PER_TRIGGER)

    parse_json_df = parse_df(kafka_df, PAYLOAD_SCHEMA)

    df_with_moves = parse_json_df.withColumn("parsed_pgn", parse_df_pgn(col("pgn")))

    return (
        df_with_moves.writeStream
        .foreachBatch(write_game_data)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/chess-games-checkpoint")
        .start()
    )

# Runs the player and game streams until either one terminates
def main() -> None:
    spark = create_spark_session()
    queries = [process_player_data(spark), process_game_data(spark)]
    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Stream failed {e}")

        for q in queries:
            if q.isActive:
                q.stop()


if __name__ == "__main__":
    main()