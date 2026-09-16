from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType, FloatType

# Envelope the collectors wrap around every Kafka payload
def envelope_schema(schema: StructType) -> StructType:
    return StructType([
    StructField("run_id", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("payload", schema, True)
])

# Rating snapshots inside a /stats response
LAST_RATING_SCHEMA = StructType([
    StructField("date", LongType(), True),
    StructField("rating", IntegerType(), True),
    StructField("rd", IntegerType(), True)
])


BEST_RATING_SCHEMA = StructType([
    StructField("date", LongType(), True),
    StructField("rating", IntegerType(), True),
    StructField("game", StringType(), True)
])


LESSON_SNAPSHOT_SCHEMA = StructType([
        StructField("rating", IntegerType(), True),
    StructField("date", TimestampType(), True)
    ])


# Per time class: current and best rating, win/loss/draw record and tournaments
RATING_BLOCK_SCHEMA = StructType([
    StructField("last", LAST_RATING_SCHEMA, True),
    StructField("best", BEST_RATING_SCHEMA, True),
    StructField(
        "record",
        StructType(
            [
                StructField("win", IntegerType(), True),
                StructField("loss", IntegerType(), True),
                StructField("draw", IntegerType(), True),
                StructField("time_per_move", IntegerType(), True),
                StructField("timeout_percent", DoubleType(), True)
            ]
        ),
        True
    ),
    StructField(
        "tournament",
        StructType(
            [
                StructField("count", IntegerType(), True),
                StructField("withdraw", IntegerType(), True),
                StructField("points", IntegerType(), True),
                StructField("highest_finish", IntegerType(), True)
            ]
        ),
        True
    )
])


# Full /stats response
STATS_SCHEMA = StructType([
    StructField("chess_blitz", RATING_BLOCK_SCHEMA, True),
    StructField("chess_rapid", RATING_BLOCK_SCHEMA, True),
    StructField("chess_bullet", RATING_BLOCK_SCHEMA, True),
    StructField(
        "tactics",
        StructType(
            [
                StructField(
                    "highest",
                    LESSON_SNAPSHOT_SCHEMA,
                    True
                ),
                StructField(
                    "lowest",
                    LESSON_SNAPSHOT_SCHEMA,
                    True
                )
            ]
        ),
        True
    ),
    StructField(
        "lessons",
        StructType(
            [
                StructField("highest", LESSON_SNAPSHOT_SCHEMA, True),
                StructField("lowest", LESSON_SNAPSHOT_SCHEMA, True)
            ]
        ),
        True
    ),
    StructField(
        "puzzle_rush",
        StructType(
            [
                StructField(
                    "daily",
                    StructType(
                        [
                            StructField("total_attempts", IntegerType(), True),
                            StructField("score", IntegerType(), True)
                        ]
                    ),
                    True
                ),
                StructField(
                    "best",
                    StructType(
                        [
                            StructField("total_attempts", IntegerType(), True),
                            StructField("score", IntegerType(), True)
                        ]
                    ),
                    True
                )
            ]
        ),
        True
    )
])


# Player message: username and country from the roster, stats from the API
PLAYER_SCHEMA = StructType([
    StructField("username", StringType(), True),
    StructField("country_hint", StringType(), True),
    StructField("stats", STATS_SCHEMA, True)
])

# One move as produced by the PGN parsing UDF
MOVE_SCHEMA = StructType([
    StructField("move_san", StringType(), True),
    StructField("remaining_clock_sec", FloatType(), True),
    StructField("ply", IntegerType(), True),
    StructField("move_number", IntegerType(), True),
    StructField("player", StringType(), True),
    StructField("time_spent_sec", FloatType(), True),
    StructField("fen_before", StringType(), True)
])

# Parsed PGN: termination and opening headers plus the move list
PGN_GAME_SCHEMA = StructType([
    StructField("termination", StringType(), True),
    StructField("eco", StringType(), True),
    StructField("moves", ArrayType(MOVE_SCHEMA), True)
])

# One side of a game record
PLAYER_INFO_SCHEMA = StructType([
    StructField("rating", IntegerType(), True),
    StructField("result", StringType(), True),
    StructField("username", StringType(), True)
])

# Game message payload from a chess.com monthly archive
PAYLOAD_SCHEMA = StructType([
    StructField("pgn", StringType(), True),
    StructField("rated", BooleanType(), True),
    StructField("uuid", StringType(), True),
    StructField("fen", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("time_class", StringType(), True),
    StructField("white", PLAYER_INFO_SCHEMA, True),
    StructField("black", PLAYER_INFO_SCHEMA, True)
])