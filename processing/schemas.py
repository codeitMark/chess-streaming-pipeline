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


# collect_players no longer calls /player/{u}, only /player/{u}/stats -- which
# carries no identity at all. The username is the roster entry it was read from
# and country_hint is the roster's own country, so both ride along in the payload
# instead of costing a second request per player.
PLAYER_SCHEMA = StructType([
    StructField("username", StringType(), True),
    StructField("country_hint", StringType(), True),
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
    StructField("time_spent_sec", FloatType(), True), # Previous clock vs current clock
    StructField("fen_before", StringType(), True)
])

PGN_GAME_SCHEMA = StructType([
    StructField("termination", StringType(), True),
    StructField("eco", StringType(), True),
    StructField("moves", ArrayType(MOVE_SCHEMA), True)
])

PLAYER_INFO_SCHEMA = StructType([
    StructField("rating", IntegerType(), True),
    StructField("result", StringType(), True),
    StructField("username", StringType(), True)
])

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