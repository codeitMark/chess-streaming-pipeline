#!/usr/bin/env python3
import os
import random
import sys
import uuid

from ingestion.common import BASE, TOPIC_PLAYERS, envelope, make_fetcher, make_producer, mongo_db

# Kafka, MongoDB target and courtesy delay between serial API requests
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DATABASE = "chess_db"
PLAYERS = "chess_players"
DELAY = 0.6


# Samples unseen players from each country roster and sends their stats to Kafka
def main() -> int:
    # CLI: comma-separated country codes, then new players per country
    raw = sys.argv[1] if len(sys.argv) > 1 else "NO"
    countries = [c.strip().upper() for c in raw.split(",") if c.strip()]
    players = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    run_id = uuid.uuid4().hex[:12]
    fetcher = make_fetcher(DELAY)
    producer = make_producer(BOOTSTRAP)
    db = mongo_db(DATABASE)

    emitted = 0
    for iso in countries:
        # One request returns the whole country roster
        listing = fetcher.get(f"{BASE}/country/{iso}/players")
        if not listing:
            print(f"[{iso}] roster unavailable")
            continue

        roster = listing.get("players", [])

        # Exclude players already stored, so repeated runs only fetch new ones
        known = {doc["_id"] for doc in db[PLAYERS].find({"country_code": iso}, {"_id": 1})}
        fresh = [u for u in roster if u.lower() not in known]

        # Random sample, as the roster comes back in a fixed alphabetical order
        window = random.sample(fresh, min(players, len(fresh)))

        for username in window:
            # Stats only, since the username and country are already known from the roster
            stats = fetcher.get(f"{BASE}/player/{username}/stats")
            if not stats:
                continue

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