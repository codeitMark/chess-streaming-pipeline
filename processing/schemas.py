from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType, FloatType

# --- Envelope Schema ---

def envelope_schema(schema: StructType) -> StructType:
    return StructType([
    StructField("run_id", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("payload", schema, True)
])

# --- Player Schema ---

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


PROFILE_SCHEMA = StructType([
    StructField("@id", StringType(), False),
    StructField("username", StringType(), False),
    StructField("player_id", LongType(), False),
    StructField("title", StringType(), True),
    StructField("status", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("joined", LongType(), True),
    StructField("last_online", LongType(), True),
    StructField("is_streamer", BooleanType(), True)
])


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


PLAYER_SCHEMA = StructType([
    StructField("profile", PROFILE_SCHEMA, True),
    StructField("stats", STATS_SCHEMA, True)
])

# --- Game Schema ---

# The fields are derived from python_chess. move_san (e.g.  e4) and remaining_clock_sec (e.g. {[%clk 0:00:00.0]}) is already in the PGN.
MOVE_SCHEMA = StructType([
    StructField("move_san", StringType(), True),
    StructField("remaining_clock_sec", FloatType(), True),
    StructField("ply", IntegerType(), True), # node.ply() from pythonchess
    StructField("move_number", IntegerType(), True), # (ply + 1) // 2 from pythonchess
    StructField("player", StringType(), True),
    StructField("time_spent_sec", FloatType(), True) # Previous clock vs current clock
])

GAME_SCHEMA = StructType([
    StructField("event", StringType(), True),
    StructField("game_link", StringType(), True),
    StructField("utc_date", StringType(), True),
    StructField("utc_time", StringType(), True),
    StructField("white", StringType(), True),
    StructField("black", StringType(), True),
    StructField("white_elo", IntegerType(), True),
    StructField("black_elo", IntegerType(), True),
    StructField("time_control", StringType(), True),
    StructField("result", StringType(), True),
    StructField("termination", StringType(), True),
    StructField("eco", StringType(), True),
    StructField("eco_url", StringType(), True),
    StructField("moves", ArrayType(MOVE_SCHEMA), True)
])

# Assumes multiple games in one payload.
MULTI_GAME_SCHEMA = StructType([
    StructField("games", ArrayType(GAME_SCHEMA), True)
])
