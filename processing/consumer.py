#!/usr/bin/env python3
"""Stage 3 -- parse and validate, Kafka to MongoDB.

Reads raw payloads, applies the filters, and writes typed documents. Every write
is an idempotent upsert keyed by `_id`, so replaying a Kafka offset range is
always safe.

Filtered-out records are counted by reason and printed at the end -- local
confirmation only, nothing about them is written to MongoDB. The filters are
deterministic and the raw payload stays on the broker for the retention window,
so any rejection is reproducible by replaying an offset range.

    python processing/consumer.py --topics players.raw,games.raw
    python processing/consumer.py --from-files ingestion/out/players_2026.../

The --from-files mode reads the collectors' .jsonl fallback output, so the whole
parse path can be tested without a broker running.
"""

from __future__ import annotations

import argparse
import os
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import UpdateOne  # noqa: E402

from ingestion.common import mongo_db  # noqa: E402
from processing import schema  # noqa: E402


class Batcher:
    """Accumulates upserts and flushes them unordered, which is both faster and
    tolerant of the duplicate keys that game archives guarantee."""

    def __init__(self, db: Any, size: int = 500) -> None:
        self.db = db
        self.size = size
        self.pending: dict[str, list[UpdateOne]] = {}
        self.written: dict[str, int] = {}

    def add(self, collection: str, doc: dict[str, Any], merge: bool = False) -> None:
        if doc.get("_id") is None:
            return
        update = {"$set": doc} if merge else {"$set": doc}
        self.pending.setdefault(collection, []).append(
            UpdateOne({"_id": doc["_id"]}, update, upsert=True)
        )
        if len(self.pending[collection]) >= self.size:
            self.flush(collection)

    def flush(self, collection: str | None = None) -> None:
        targets = [collection] if collection else list(self.pending)
        for name in targets:
            ops = self.pending.get(name)
            if not ops:
                continue
            result = self.db[name].bulk_write(ops, ordered=False)
            self.written[name] = self.written.get(name, 0) + result.upserted_count + result.modified_count
            self.pending[name] = []


def handle_player(
    message: dict[str, Any], batch: Batcher, today: str, reasons: Counter
) -> None:
    payload = message["payload"]
    profile, stats = payload.get("profile") or {}, payload.get("stats") or {}
    player = schema.build_player(profile, stats, today)
    if not player["_id"]:
        reasons["no_username"] += 1
        return

    batch.pending.setdefault(schema.PLAYERS, []).append(
        UpdateOne(
            {"_id": player["_id"]},
            {"$set": player, "$setOnInsert": {"games_through": None}},
            upsert=True,
        )
    )


def handle_games(
    message: dict[str, Any], batch: Batcher, floor: int, cache: set[str],
    reasons: Counter,
) -> tuple[int, int]:
    """Returns (kept, rejected). Rejections are counted, not stored."""
    kept = rejected = 0
    for game in (message["payload"] or {}).get("games", []):
        reason = schema.game_reject_reason(game, floor)
        if reason:
            reasons[reason] += 1
            rejected += 1
            continue

        white_id = ensure_player(game.get("white") or {}, batch, cache)
        black_id = ensure_player(game.get("black") or {}, batch, cache)
        if white_id is None or black_id is None:
            reasons["no_username"] += 1
            rejected += 1
            continue

        doc = schema.build_game(game, white_id, black_id)
        batch.add(schema.GAMES, doc)

        parsed = schema.parse_movetext(game.get("pgn") or "", doc["base"], doc["increment"])
        if parsed is None:
            reasons["unparseable_pgn"] += 1
        else:
            batch.add(schema.GAME_MOVES, schema.build_moves(doc["_id"], parsed))
        kept += 1
    return kept, rejected


def ensure_player(ref: dict[str, Any], batch: Batcher, cache: set[str]) -> str | None:
    """Return the player key for a game participant, creating a stub if needed.

    Opponents are almost never in the sampled cohort, so a stub is written for
    them the first time they appear. `$setOnInsert` means a stub never overwrites
    a real profiled player, whichever order the messages arrive in. The cache
    only suppresses repeat writes -- archives repeat opponents heavily.
    """
    stub = schema.build_stub_player(ref)
    if stub is None:
        return None
    if stub["_id"] not in cache:
        cache.add(stub["_id"])
        batch.pending.setdefault(schema.PLAYERS, []).append(
            UpdateOne({"_id": stub["_id"]}, {"$setOnInsert": stub}, upsert=True)
        )
    return stub["_id"]


def read_files(directory: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    for path in sorted(directory.glob("*.jsonl")):
        topic = path.stem
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                yield topic, json.loads(line)["value"]


def read_kafka(topics: list[str], bootstrap: str, group: str, timeout: int) -> Iterator:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap.split(","),
        group_id=group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # Offsets are committed only after a successful flush, so a crash
        # re-delivers rather than loses. The upserts make that safe.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout * 1000,
        max_partition_fetch_bytes=8 * 1024 * 1024,
    )
    for record in consumer:
        yield record.topic, record.value
    consumer.commit()
    consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse raw payloads into MongoDB.")
    parser.add_argument("--topics", default=f"{schema.TOPIC_PLAYERS},{schema.TOPIC_GAMES}")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        help="Defaults to $KAFKA_BOOTSTRAP_SERVERS. Inside compose that is "
             "kafka:9092; from the host it is localhost:9094.",
    )
    parser.add_argument("--group", default="parse-validate")
    parser.add_argument("--from-files", default=None, help="Read collector .jsonl output instead of Kafka.")
    parser.add_argument("--timeout", type=int, default=30, help="Seconds of idle before exiting.")
    parser.add_argument("--since", default=None, help="Drop games ending before this YYYY-MM-DD.")
    parser.add_argument("--indexes", action="store_true", help="Create indexes and exit.")
    args = parser.parse_args()

    db = mongo_db()

    if args.indexes:
        for name in schema.ensure_indexes(db):
            print(f"  {name}")
        return 0

    floor = 0
    if args.since:
        from datetime import datetime, timezone

        floor = int(datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc).timestamp())

    today = date.today().isoformat()
    batch = Batcher(db)
    cache: set[str] = set()
    reasons: Counter[str] = Counter()
    kept = rejected = seen = 0

    source = (
        read_files(Path(args.from_files))
        if args.from_files
        else read_kafka(args.topics.split(","), args.bootstrap, args.group, args.timeout)
    )

    for topic, message in source:
        seen += 1
        if topic == schema.TOPIC_PLAYERS:
            handle_player(message, batch, today, reasons)
        elif topic == schema.TOPIC_GAMES:
            k, r = handle_games(message, batch, floor, cache, reasons)
            kept += k
            rejected += r
        if seen % 100 == 0:
            print(f"  {seen} messages | {kept} games kept | {rejected} rejected")

    batch.flush()
    print(f"\n{seen} messages consumed")
    for name, count in sorted(batch.written.items()):
        print(f"  {name:<18} {count:>8} upserts")
    if reasons:
        # Local confirmation only -- nothing about rejections is written to Mongo.
        print(f"  filtered out {rejected}:")
        for reason, count in reasons.most_common():
            print(f"    {reason:<24} {count:>8}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
