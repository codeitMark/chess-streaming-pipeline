import os
import argparse
from functools import partial
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, from_json, coalesce, lit, when, to_date, date_format, explode, size, element_at, reverse, transform
from processing.schemas import PLAYER_SCHEMA, envelope_schema
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
def read_player_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC_PLAYERS)
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

    kafka_df = read_player_stream(spark)

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


def main() -> None:
    streaming_query = process_player_data()
    streaming_query.awaitTermination() # since .start() is asynchronous (for streaming real-time), we need to awaitTermination for it to finish processing all the data and writing it to MongoDB before exiting the python process.


if __name__ == "__main__":
    main()