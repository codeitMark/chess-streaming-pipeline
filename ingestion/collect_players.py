#!/usr/bin/env python3
import os
import sys
import uuid

from ingestion.common import BASE, TOPIC_PLAYERS, envelope, make_fetcher, make_producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = 1.0


def main() -> int:
    raw = sys.argv[1] if len(sys.argv) > 1 else "NO"
    countries = [c.strip().upper() for c in raw.split(",") if c.strip()]
    players = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    run_id = uuid.uuid4().hex[:12]
    fetcher = make_fetcher(DELAY)
    producer = make_producer(BOOTSTRAP)

    print(f"run {run_id} | {countries} | {players} each\n")

    emitted = 0
    for iso in countries:
        # One request gives the whole country roster.
        listing = fetcher.get(f"{BASE}/country/{iso}/players")
        if not listing:
            print(f"[{iso}] roster unavailable")
            continue

        window = listing.get("players", [])[:players]
        print(f"[{iso}] taking {len(window)} players")

        for username in window:
            # Two requests per player: profile, then stats.
            profile = fetcher.get(f"{BASE}/player/{username}")
            if not profile:
                continue
            stats = fetcher.get(f"{BASE}/player/{username}/stats") or {}

            # One message per player, so the consumer writes one document.
            producer.send(
                TOPIC_PLAYERS,
                key=username,
                value=envelope(
                    run_id,
                    f"{BASE}/player/{username}",
                    {"profile": profile, "stats": stats, "country_hint": iso},
                ),
            )
            emitted += 1

    producer.flush()
    producer.close()
    print(f"\n{emitted} players sent | {fetcher.requests_made} requests "
          f"| {fetcher.raw_bytes:,} B")
    return 0


if __name__ == "__main__":
    sys.exit(main())