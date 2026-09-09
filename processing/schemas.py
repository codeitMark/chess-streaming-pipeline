from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

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

"""

[Event "Live Chess"]
[Site "Chess.com"]
[Date "2026.05.29"]
[Round "-"]
[White "MagnusCarlsen"]
[Black "Resentiment18"]
[Result "1/2-1/2"]
[CurrentPosition "8/5pk1/1p4p1/p1p1p3/P1PbP1B1/1P4P1/5PK1/8 b - - 4 39"]
[Timezone "UTC"]
[ECO "B90"]
[ECOUrl "https://www.chess.com/openings/Sicilian-Defense-Open-Najdorf-Variation-6.h4-e5-7.Nb3"]
[UTCDate "2026.05.29"]
[UTCTime "13:32:14"]
[WhiteElo "3373"]
[BlackElo "3036"]
[TimeControl "180"]
[Termination "Game drawn by agreement"]
[StartTime "13:32:14"]
[EndDate "2026.05.29"]
[EndTime "13:37:30"]
[Link "https://www.chess.com/game/live/169411269728"]

1. e4 {[%clk 0:02:59.5]} 1... c5 {[%clk 0:02:57.8]} 2. Nf3 {[%clk 0:02:58.2]} 2... d6 {[%clk 0:02:57.2]} 3. d4 {[%clk 0:02:56.9]} 3... cxd4 {[%clk 0:02:56.5]} 4. Nxd4 {[%clk 0:02:56.8]} 4... Nf6 {[%clk 0:02:55.9]} 5. Nc3 {[%clk 0:02:56.2]} 5... a6 {[%clk 0:02:55.4]} 6. h4 {[%clk 0:02:53.7]} 6... e5 {[%clk 0:02:50.2]} 7. Nb3 {[%clk 0:02:51.8]} 7... Be7 {[%clk 0:02:48.4]} 8. Bg5 {[%clk 0:02:51.1]} 8... O-O {[%clk 0:02:42.5]} 9. Bxf6 {[%clk 0:02:48.4]} 9... Bxf6 {[%clk 0:02:41.5]} 10. Nd5 {[%clk 0:02:48.3]} 10... Nc6 {[%clk 0:02:35.6]} 11. g3 {[%clk 0:02:47]} 11... Be6 {[%clk 0:02:31.8]} 12. Bh3 {[%clk 0:02:43.6]} 12... a5 {[%clk 0:02:13.8]} 13. a4 {[%clk 0:02:40.4]} 13... Bxd5 {[%clk 0:02:08]} 14. Qxd5 {[%clk 0:02:38.7]} 14... Nb4 {[%clk 0:02:07.3]} 15. Qd1 {[%clk 0:02:35.7]} 15... Qb6 {[%clk 0:02:00.8]} 16. c3 {[%clk 0:02:34.4]} 16... Na6 {[%clk 0:01:54.5]} 17. O-O {[%clk 0:02:28.3]} 17... Rad8 {[%clk 0:01:53.1]} 18. Qd5 {[%clk 0:02:22.4]} 18... Nc5 {[%clk 0:01:51.6]} 19. Nxc5 {[%clk 0:02:20.8]} 19... dxc5 {[%clk 0:01:48]} 20. Qc4 {[%clk 0:02:15.7]} 20... Rd2 {[%clk 0:01:46]} 21. Rad1 {[%clk 0:02:10.4]} 21... Rfd8 {[%clk 0:01:44.7]} 22. Rxd2 {[%clk 0:02:06.5]} 22... Rxd2 {[%clk 0:01:44.6]} 23. Bg4 {[%clk 0:02:03.3]} 23... g6 {[%clk 0:01:26.4]} 24. h5 {[%clk 0:02:02.3]} 24... Kg7 {[%clk 0:01:24.7]} 25. b3 {[%clk 0:01:51.4]} 25... Bg5 {[%clk 0:01:22]} 26. Kg2 {[%clk 0:01:50.2]} 26... Qd6 {[%clk 0:01:15.9]} 27. Qb5 {[%clk 0:01:44.5]} 27... b6 {[%clk 0:01:15]} 28. Be2 {[%clk 0:01:35.7]} 28... Qf6 {[%clk 0:01:03.6]} 29. Rd1 {[%clk 0:01:33]} 29... Qd6 {[%clk 0:00:57.6]} 30. Rxd2 {[%clk 0:01:29.1]} 30... Qxd2 {[%clk 0:00:56.6]} 31. Qc4 {[%clk 0:01:20.4]} 31... Bh6 {[%clk 0:00:43.3]} 32. Qa6 {[%clk 0:01:08.9]} 32... Bg5 {[%clk 0:00:41.5]} 33. Qb5 {[%clk 0:00:33.7]} 33... Qc2 {[%clk 0:00:37.4]} 34. Qd3 {[%clk 0:00:28]} 34... Qxd3 {[%clk 0:00:36.2]} 35. Bxd3 {[%clk 0:00:27.9]} 35... Bd2 {[%clk 0:00:35.3]} 36. hxg6 {[%clk 0:00:26.9]} 36... hxg6 {[%clk 0:00:33.7]} 37. c4 {[%clk 0:00:26.8]} 37... Bc3 {[%clk 0:00:30.8]} 38. Be2 {[%clk 0:00:26.7]} 38... Bd4 {[%clk 0:00:30.4]} 39. Bg4 {[%clk 0:00:24.1]} 1/2-1/2


[Event "Live Chess"]
[Site "Chess.com"]
[Date "2026.05.29"]
[Round "-"]
[White "Resentiment18"]
[Black "MagnusCarlsen"]
[Result "0-1"]
[CurrentPosition "r4rk1/p3bppp/3p1n2/q4PP1/1n3B2/PppQ1P2/1PP1N2P/1K1R1B1R w - - 1 18"]
[Timezone "UTC"]
[ECO "C41"]
[ECOUrl "https://www.chess.com/openings/Philidor-Defense-Exchange-Variation...6.Bf4-O-O-7.Qd2-c6-8.O-O-O"]
[UTCDate "2026.05.29"]
[UTCTime "13:29:47"]
"""

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
    StructField("event", StringType(), True),            # [Event ...]
    StructField("game_link", StringType(), True),        # [Link ...]
    StructField("utc_date", StringType(), True),         # [UTCDate ...]
    StructField("utc_time", StringType(), True),         # [UTCTime ...]
    StructField("white", StringType(), True),
    StructField("black", StringType(), True),
    StructField("white_elo", IntegerType(), True),       # [WhiteElo ...]
    StructField("black_elo", IntegerType(), True),       # [BlackElo ...]
    StructField("time_control", StringType(), True),     # [TimeControl ...]
    StructField("result", StringType(), True),           # [Result ...]
    StructField("termination", StringType(), True),      # [Termination ...]
    StructField("eco", StringType(), True),              # [ECO ...]
    StructField("eco_url", StringType(), True),          # [ECOUrl ...]
    StructField("moves", ArrayType(MOVE_SCHEMA), True)   # Move Array
])

# Assumes multiple games in one payload.
MULTI_GAME_SCHEMA = StructType([
    StructField("games", ArrayType(GAME_RECORD_SCHEMA), True)
])

"""
[WhiteElo "3030"]
[BlackElo "3379"]
[TimeControl "180"]
[Termination "MagnusCarlsen won by resignation"]
[StartTime "13:29:47"]
[EndDate "2026.05.29"]
[EndTime "13:32:02"]
[Link "https://www.chess.com/game/live/169411164842"]

1. e4 {[%clk 0:02:58.5]} 1... d6 {[%clk 0:02:58.3]} 2. d4 {[%clk 0:02:57.4]} 2... Nf6 {[%clk 0:02:57.9]} 3. Nc3 {[%clk 0:02:54.7]} 3... e5 {[%clk 0:02:57]} 4. Nf3 {[%clk 0:02:53.3]} 4... exd4 {[%clk 0:02:55.4]} 5. Nxd4 {[%clk 0:02:50.5]} 5... Be7 {[%clk 0:02:55.3]} 6. Bf4 {[%clk 0:02:49.7]} 6... O-O {[%clk 0:02:54]} 7. Qd2 {[%clk 0:02:49.4]} 7... c6 {[%clk 0:02:50.3]} 8. O-O-O {[%clk 0:02:43.9]} 8... b5 {[%clk 0:02:49.7]} 9. f3 {[%clk 0:02:41.5]} 9... b4 {[%clk 0:02:49]} 10. Nce2 {[%clk 0:02:38.3]} 10... c5 {[%clk 0:02:47.8]} 11. Nf5 {[%clk 0:02:28.5]} 11... Bxf5 {[%clk 0:02:46.7]} 12. exf5 {[%clk 0:02:28.4]} 12... Nc6 {[%clk 0:02:44.4]} 13. g4 {[%clk 0:02:27.1]} 13... Qa5 {[%clk 0:02:38.5]} 14. Kb1 {[%clk 0:02:26.2]} 14... c4 {[%clk 0:02:34.5]} 15. g5 {[%clk 0:02:19.6]} 15... c3 {[%clk 0:02:33.1]} 16. Qd3 {[%clk 0:02:11]} 16... b3 {[%clk 0:02:32.1]} 17. a3 {[%clk 0:01:30.5]} 17... Nb4 {[%clk 0:02:27.4]} 0-1
"""