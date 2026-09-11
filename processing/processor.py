import os
import io
import re
import argparse
import chess.pgn
from functools import partial
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, from_json, coalesce, lit, explode, udf
from processing.schemas import PLAYER_SCHEMA, PAYLOAD_SCHEMA, PGN_GAME_SCHEMA, envelope_schema
from ingestion.common import TOPIC_PLAYERS, TOPIC_GAMES, mongo_db
from pymongo import UpdateOne

#To run use 'docker compose exec python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --master spark://spark-master:7077 /CS4010/processing/processor.py'

# spark cant find kafka so must use a flag instead (as this mostly applies to python only). I have moved master off as well, due to the inconsistencies here, and define it in the command above.
'''
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
        )
'''

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DATABASE = "chess_db"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("chess-streaming-processor")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.log.level", "WARN") # Spark log level WARN so it doesnt spam the terminal with INFO logs that write nothing after being done with microbatches...
        .getOrCreate()
    )
    return spark

# Using streaming, as we will be using real-time streaming. I do not see a point in using batch processing when we are upserting and not using historical data (which we would need to populate or backfill)
def read_stream(spark: SparkSession, const_topic) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", const_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

# batch_id is required for foreachBatch even though we dont use it directly. id_path is for MongoDB to find an id it will to identify the document that is being written. tuple contains 1 or more strings.
def write_to_mongodb(batch_df: DataFrame, batch_id: int, collection_name: str, id_path: tuple[str, ...]) -> None:
    operations = [] # store all mongodb operations/updates in a list. uses list to bulk_write into mongodb.

    # toLocalIterator preferred compared to collect() for big amounts of data
    for row in batch_df.toLocalIterator():
        document = row.asDict(recursive=True) # converts the rows into dictionaries. necessary for x. requires recursive=True because of the nested rows.
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

    db = mongo_db(DATABASE)
    collection = db[collection_name]
    collection.bulk_write(operations, ordered=False)

# Parses Kafka JSON to columns by any schema. send in false as the 3rd parameter if it is not enveloped (aka data at root rather than nested in a payload).
def parse_df(kafka_df: DataFrame, schema: StructType, enveloped: bool = True) -> DataFrame:
    json_column = col("value").cast("string")

    if enveloped:
        parsed = kafka_df.select(
            from_json(json_column, envelope_schema(schema)).alias("message"), # message is the entire content of the envelope
        )
        return parsed.select("message.payload.*", "message.fetched_at")

    return kafka_df.select(from_json(json_column, schema).alias("data")).select("data.*")

def process_player_data(spark: SparkSession) -> StreamingQuery:
    kafka_df = read_stream(spark, TOPIC_PLAYERS)

    parsed_df = parse_df(kafka_df, PLAYER_SCHEMA)

    processed_df = (
    # null-handling
        parsed_df
        .filter(col("profile").isNotNull())
        .filter(col("stats").isNotNull())
        .filter(col("profile").getField("@id").isNotNull())
        .filter(col("profile").getField("username").isNotNull())
        .filter(col("profile").getField("player_id").isNotNull())
    )

    # counts total game for each gamemode, then sums them into total_games_played. gives us 4 new columns.
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

    # partial because we don't have anything for the batch_id parameter
    player_writer = partial(
        write_to_mongodb,
        collection_name="chess_players",
        id_path=("profile", "player_id") # uses player_id as MongoDB's _id.
    )

    return (
        processed_df.writeStream
        .foreachBatch(player_writer) #uses both batch_df and batch_id
        .outputMode("append")
        .option("checkpointLocation", "/tmp/chess-player-checkpoint")
        .start()
    )

def clock_to_seconds(clk_str):
    if not clk_str:
        return None
    parts = clk_str.split(":")
    if len(parts) == 3:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2]) # hours
    elif len(parts) == 2:
        return float(parts[0]) * 60 + float(parts[1]) # minutes
    return float(parts[0]) # only seconds

# PGN Parser UDF. Spark does not support PGN through DataFrames.
@udf(returnType=PGN_GAME_SCHEMA)
def parse_df_pgn(raw_pgn_str):
    if not raw_pgn_str:
        return None

    pgn_io = io.StringIO(raw_pgn_str) #pgn.read_game() expects a stream, not raw string. therefore needs this to be trasnformed.
    game = chess.pgn.read_game(pgn_io)
    if game is None:
        return None

    clk = re.compile(r"\[%clk\s+([\d:\.]+)\]") # extracts the time with regex. returns a string with only the time and not [, ], and %clk

    headers = game.headers
        
    # Determine starting base time control for move 1 time_spent calculation
    tc_str = headers.get("TimeControl", "180") # Checks the time in the game, 180 is set as default (e.g. 3 min blitz).
    base_tc = float(tc_str.split("+")[0]) if tc_str.split("+")[0].isdigit() else 180.0 # base time control (for cases with e.g. 180+2 (180 secs with +2 sec increments))

    parsed_moves = []
    board = game.board()
    prev_clocks = {"white": base_tc, "black": base_tc} # stores most recent clock readings, necessary for calculating time spent during each move.

        # goes through every move (node) in the game. ply is every (half) move, e.g. move 2 for black and white is ply 3 and 4.
    for node in game.mainline():
        ply = node.ply()
        move_num = (ply + 1) // 2
        fen_before = board.fen()
        
        # Determine player who played THIS node
        player = "white" if board.turn == chess.WHITE else "black"
        move_san = board.san(node.move) # Converts raw coordinates to standard notation (e.g. Nf3)
        
        # node.comment is what's inside {}, e.g. [00:00:00.0], which we extract by using regex clk
        match = clk.search(node.comment)

        # Initialized early with None in case there is missing data (e.g. no time control)
        remaining_sec = None
        time_spent_sec = None
            
        if match:
            remaining_sec = clock_to_seconds(match.group(1)) # match.group(1) returns only the time, while group(0) returns the entire string ([%clk 00:00:00.0])
            if prev_clocks[player] is not None:
                time_spent_sec = max(0.0, round(prev_clocks[player] - remaining_sec, 2))
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

        # Next (half) move
        board.push(node.move)

    return {
        "termination": headers.get("Termination"),
        "eco": headers.get("ECO"),
        "moves": parsed_moves
    }

# Assumes the input is through collect_games.py, as that sends a json file (PGN file + envelope overhead).
def process_game_data(spark: SparkSession) -> StreamingQuery:
    kafka_df = read_stream(spark, TOPIC_GAMES)

    parse_json_df = parse_df(kafka_df, PAYLOAD_SCHEMA)

    df_with_moves = parse_json_df.withColumn("parsed_pgn", parse_df_pgn(col("pgn")))

    # Need to explode it twice (games and moves). It will store each move in it's own document with metadata to identify the game. Makes fetching/filtering in MongoDB and analysis easier (e.g. "find all blunders under 5 seconds")
    final_df = (df_with_moves.select(
        col("uuid").alias("_id"), # MongoDB primary key
        col("fetched_at"),
        col("rated"),
        col("time_control"),
        col("time_class"),
        col("fen").alias("final_fen"),

        # additional headers from the PGN
        col("parsed_pgn.eco").alias("eco"),
        col("parsed_pgn.termination").alias("termination"),

        col("white.username").alias("white_username"),
        col("white.rating").alias("white_rating"),
        col("white.result").alias("white_result"),
        col("black.username").alias("black_username"),
        col("black.rating").alias("black_rating"),
        col("black.result").alias("black_result"),

        explode(col("parsed_pgn.moves")).alias("move")
        ).select(
            "*",
            col("move.ply").alias("ply"),
            col("move.move_number").alias("move_number"),
            col("move.player").alias("player"),
            col("move.move_san").alias("move_san"),
            col("move.remaining_clock_sec").alias("remaining_clock_sec"),
            col("move.time_spent_sec").alias("time_spent_sec"),
            col("move.fen_before").alias("fen_before")
        )
        .withColumn(
            "_id", 
            concat_ws("_ply_", col("_id"), col("ply").cast("string"))
        )
        .drop("move", "parsed_pgn", "pgn") # Need to drop these since we exploded and flattened the data. These would be duplicates.
    )

    games_writer = partial(
        write_to_mongodb,
        collection_name="chess_games",
        id_path=("_id",) # Requires a tuple, so sending it as a tuple.
    )

    return (
        final_df.writeStream
        .foreachBatch(games_writer) # this is where both batch_df and batch_id come in. These are automatically supplied by Spark.
        .outputMode("append")
        .option("checkpointLocation", "/tmp/chess-games-checkpoint")
        .start()
    )

def evaluate_partition(iterator):
    for row in iterator:
        row_dict = row.asDict()
        
        # Access the single game data
        pgn_string = row_dict["game"]["moves"] 
        
        # Pass the raw PGN string of the single game to your teammate's function
        #evaluation_result = stockfish_processing(pgn_string)
        
        # Yield the output to be collected into a new DataFrame (Uses this as a schema, we end up with the PGN string and evaluation_result)
        yield Row(
            pgn=pgn_string  #,
            #evaluation=evaluation_result
        )

def process_micro_batch(batch_df, batch_id):
    eval_rdd = batch_df.rdd.mapPartitions(evaluate_partition)

    eval_df = batch_df.sparkSession.createDataFrame(eval_rdd)

    eval_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("collection", "chess_evaluations") \
        .save()

def evaluate_game_data(spark: SparkSession):
    kafka_df = read_stream(spark, TOPIC_GAMES)

    df_pgn = kafka_df.select(col("value").cast("string").alias("raw_pgn"))

    parsed_df = df_pgn.withColumn("parsed_data", parse_df_pgn(col("raw_pgn")))

    single_games_df = parsed_df.select(explode(col("games")).alias("game"))

    eval_games_writer = partial(
        write_to_mongodb,
        collection_name="evaluated_chess_games",
        id_path=("_id",) # Requires a tuple, so sending it as a tuple.
    )

    return (
        single_games_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", "/tmp/eval-chess-games-checkpoint")
        .start()
    )

def main() -> None:
    spark = create_spark_session()
    queries = [process_player_data(spark), process_game_data(spark)]
    try:
        spark.streams.awaitAnyTermination() # since .start() is asynchronous (for streaming real-time), we need to awaitTermination for it to finish processing all the data and writing it to MongoDB before exiting the python process.
    except Exception as e:
        print(f"Stream failed {e}")

        for q in queries:
            if q.isActive:
                q.stop()


if __name__ == "__main__":
    main()