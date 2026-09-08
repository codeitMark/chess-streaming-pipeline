#!/usr/bin/env python3
""" python -m processing.consumer            # consume until idle
    python -m processing.consumer --indexes  # create indexes and exit
"""

import json
import os
import sys
from collections import Counter
from datetime import date

from pymongo import UpdateOne

from ingestion.common import TOPIC_PLAYERS, TOPIC_GAMES, mongo_db
from processing import schema

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 500))
GROUP = "parse-validate"
IDLE_TIMEOUT = 30  # seconds without a message before we stop


class Batcher:
    """Queues upserts and flushes them unordered, which tolerates the duplicate
    keys that game archives guarantee."""

    def __init__(self, db):
        self.db = db
        self.pending = {}
        self.written = Counter()

    def add(self, collection, _id, update):
        if not _id:
            return

        ops = self.pending.setdefault(collection, [])
        ops.append(UpdateOne({"_id": _id}, update, upsert=True))
        if len(ops) >= BATCH_SIZE:
            self.flush(collection)

    def flush(self, collection=None):
        for name in [collection] if collection else list(self.pending):
            ops = self.pending.pop(name, None)
            if ops:
                result = self.db[name].bulk_write(ops, ordered=False)
                self.written[name] += result.upserted_count + result.modified_count


def ensure_player(ref, batch, cache) -> str | None:
    """Stub for an opponent on first sight. The cache suppresses repeat writes,
    archives repeat opponents heavily."""
    stub = schema.build_stub_player(ref)
    if stub is None:
        return None

    key = stub.pop("_id")
    if key not in cache:
        cache.add(key)
        batch.add(schema.PLAYERS, key, {"$setOnInsert": stub})

    return key


def handle_games(message, batch, cache, reasons) -> tuple[int, int]:
    """Returns (kept, rejected)."""
    kept = rejected = 0

    for game in (message["payload"] or {}).get("games", []):
        reason = schema.game_reject_reason(game)
        if reason:
            reasons[reason] += 1
            rejected += 1
            continue

        white = ensure_player(game.get("white") or {}, batch, cache)
        black = ensure_player(game.get("black") or {}, batch, cache)
        if not white or not black:
            reasons["no_username"] += 1
            rejected += 1
            continue

        doc = schema.build_game(game, white, black)
        key = doc.pop("_id")
        if key is None:
            reasons["no_game_id"] += 1
            rejected += 1
            continue

        batch.add(schema.GAMES, key, {"$set": doc})
        batch.add(schema.GAMES, key, {"$set": doc})

        parsed = schema.parse_movetext(game.get("pgn") or "", doc["base"], doc["increment"])
        if parsed is None:
            reasons["unparseable_pgn"] += 1
        else:
            batch.add(schema.GAME_MOVES, key, {"$set": parsed})

        kept += 1

    return kept, rejected


def open_consumer():
    from kafka import KafkaConsumer

    return KafkaConsumer(
        TOPIC_PLAYERS,
        TOPIC_GAMES,
        bootstrap_servers=BOOTSTRAP.split(","),
        group_id=GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=IDLE_TIMEOUT * 1000,
        max_partition_fetch_bytes=8 * 1024 * 1024,
    )


def main():
    db = mongo_db()

    if "--indexes" in sys.argv:
        for name in schema.ensure_indexes(db):
            print(f"  {name}")
        return

    today = date.today().isoformat()
    batch = Batcher(db)
    cache = set()
    reasons = Counter()
    kept = rejected = seen = 0

    consumer = open_consumer()
    for record in consumer:
        seen += 1

        if record.topic == TOPIC_PLAYERS:
            payload = record.value["payload"]
            player = schema.build_player(
                payload.get("profile") or {}, payload.get("stats") or {}, today
            )
            batch.add(
                schema.PLAYERS,
                player.pop("_id"),
                {"$set": player, "$setOnInsert": {"games_through": None}},
            )
        else:
            k, r = handle_games(record.value, batch, cache, reasons)
            kept += k
            rejected += r

        if seen % 100 == 0:
            print(f"  {seen} messages | {kept} kept | {rejected} rejected")

    batch.flush()
    consumer.commit()  # after the flush, so a crash re-delivers rather than loses
    consumer.close()

    print(f"\n{seen} messages | {kept} games kept | {rejected} rejected")
    for name, count in sorted(batch.written.items()):
        print(f"  {name:<12} {count:>8} upserts")
    for reason, count in reasons.most_common():
        print(f"  reject {reason:<20} {count:>8}")


if __name__ == "__main__":
    main()