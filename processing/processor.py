import os
import argparse
from functools import partial
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, from_json, coalesce, lit, when, to_date, date_format, explode, size, element_at, reverse, transform
from processing.schemas import PLAYER_SCHEMA, MULTI_GAME_SCHEMA, envelope_schema
from ingestion.common import TOPIC_PLAYERS, TOPIC_GAMES, mongo_db
from pymongo import UpdateOne

import io
import re
import chess.pgn
from pyspark.sql.functions import udf

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

def process_player_data() -> StreamingQuery:
    spark = create_spark_session()

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

# PGN Parser UDF. Spark does not support PGN through DataFrames. Need to explode it twice (games and moves). It will store each move in it's own document with metadata to identify the game. Makes fetching/filtering in MongoDB and analysis easier (e.g. "find all blunders under 5 seconds")
@udf(returnType=MULTI_GAME_SCHEMA)
def parse_df_pgn(raw_pgn_str):
    if not raw_pgn_str:
        return []

    pgn_io = io.StringIO(raw_pgn_str) #pgnn.read_game() expects a stream, not raw string. therefore needs this to be trasnformed.
    parsed_games = []
    clk = re.compile(r"\[%clk\s+([\d:\.]+)\]") # extracts the time with regex. returns a string with only the time and not [, ], and %clk

    # Loop through all games in the PGN string
    while True:
        game = chess.pgn.read_game(pgn_io)
        if game is None:
            break  # No (more) games

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
                "time_spent_sec": time_spent_sec
            })

            # Next (half) move
            board.push(node.move)

        # Some games (e.g. friendly matches) may not display any elo.
        parsed_games.append({
            "event": headers.get("Event"),
            "game_link": headers.get("Link"),
            "utc_date": headers.get("UTCDate"),
            "utc_time": headers.get("UTCTime"),
            "white": headers.get("White"),
            "black": headers.get("Black"),
            "white_elo": int(headers["WhiteElo"]) if headers.get("WhiteElo", "").isdigit() else None,
            "black_elo": int(headers["BlackElo"]) if headers.get("BlackElo", "").isdigit() else None,
            "time_control": headers.get("TimeControl"),
            "result": headers.get("Result"),
            "termination": headers.get("Termination"),
            "eco": headers.get("ECO"),
            "eco_url": headers.get("ECOUrl"),
            "moves": parsed_moves
        })

    return parsed_games

def process_game_data() -> StreamingQuery:
    spark = create_spark_session()

    kafka_df = read_stream(spark, TOPIC_GAMES)

    df_pgn = kafka_df.select(col("value").cast("string").alias("raw_pgn"))

    parsed_df = df_pgn.withColumn("games", parse_df_pgn(col("raw_pgn"))) # games is a column that holds all the pulled games. 

def main() -> None:
    streaming_query = process_player_data()
    streaming_query.awaitTermination() # since .start() is asynchronous (for streaming real-time), we need to awaitTermination for it to finish processing all the data and writing it to MongoDB before exiting the python process.


if __name__ == "__main__":
    main()