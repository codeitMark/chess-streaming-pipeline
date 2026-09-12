#!/usr/bin/env python3
import os
import random
import sys
import time
import uuid

from ingestion.common import BASE, TOPIC_PLAYERS, envelope, make_fetcher, make_producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = 0.6 # Chess.com rate-limits parallel requests, not serial ones. We stay serial and keep a smaller courtesy gap; 0.6s is the floor we are willing to run at.


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

        roster = listing.get("players", [])

        # Sampled, not sliced. The API caps a roster at "the first 10000 players
        # for performance reasons" and hands them back in a fixed order, so
        # taking a prefix would draw every cohort from the same alphabetical
        # head -- the 0s and 1s -- in every country, every run.
        window = random.sample(roster, min(players, len(roster)))
        print(f"[{iso}] sampling {len(window)} of {len(roster)} players")

        started = time.time()
        for done, username in enumerate(window, 1):
            # A 2000-player country is ~44 minutes of requests. Without this the
            # run prints one line per COUNTRY and looks hung in between.
            if done % 100 == 0:
                rate = done / max(time.time() - started, 1e-9)
                remaining = (len(window) - done) / rate / 60
                print(
                    f"  [{iso}] {done:>5}/{len(window)} | {rate:.2f} players/s | ~{remaining:.0f} min left",
                    flush=True,
                )

            # ONE request per player. The profile endpoint is deliberately not
            # called: the only two fields we still keep from it are the username
            # (which IS the roster entry) and the country (which is the roster we
            # are reading), so paying a second request for them would double the
            # sweep -- 63 hours instead of 32 across core Europe.
            stats = fetcher.get(f"{BASE}/player/{username}/stats")
            if not stats:
                continue

            # One message per player, so the consumer writes one document. The
            # username travels in the payload because /stats carries no identity
            # of its own.
            producer.send(
                TOPIC_PLAYERS,
                key=username,
                value=envelope(
                    run_id,
                    f"{BASE}/player/{username}/stats",
                    {"username": username, "country_hint": iso, "stats": stats},
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