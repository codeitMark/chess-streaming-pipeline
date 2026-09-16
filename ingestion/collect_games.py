from datetime import datetime, timezone
import os
import random
import sys
from typing import Any
import uuid

from ingestion.common import BASE, TOPIC_GAMES, envelope, make_fetcher, make_producer, mongo_db

# MongoDB target, Kafka and courtesy delay between serial API requests
DATABASE = "chess_db"
PLAYERS = "chess_players"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = 0.6

# Sampling rules: rating bands, accepted time classes, games per player and archive month (YYYY/MM)
RATING_BANDS = ((0, 800), (800, 1200), (1200, 1600), (1600, 2000), (2000, 5000))
LIVE_CLASSES = ("blitz", "rapid", "bullet")
MAX_GAMES_PER_PLAYER = 20
ARCHIVE_MONTH = "2026/08"


# Rejects variants, non-live time classes and unrated games before they reach Kafka
def game_reject_reason(game: dict[str, Any]) -> str | None:
    if game.get("rules") != "chess":
        return f"variant:{game.get('rules')}"

    if game.get("time_class") not in LIVE_CLASSES:
        return f"time_class:{game.get('time_class')}"

    if not game.get("rated"):
        return "unrated"

    return None


# Picks players not yet collected this month, spread evenly across rating bands
def select_players(db: Any, country: str | None, limit: int) -> list[dict]:
    # Only players whose games were not already collected this month
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    unprocessed_filter = {
        "$or": [
            {"games_through": {"$exists": False}},
            {"games_through": {"$ne": current_month}}
        ]
    }

    # Optional country restriction
    country_filter = {}
    if country:
        country_filter = {"country_code": country.upper().strip()}

    per_band = max(1, limit // len(RATING_BANDS))
    picked: list[dict] = []
    seen_ids: set[Any] = set()

    # Equal quota from each rating band
    for low, high in RATING_BANDS:
        if len(picked) >= limit:
            break

        quota = min(per_band, limit - len(picked))

        rating_query = {"blitz_rating_current": {"$gte": low, "$lt": high}}

        conditions = [unprocessed_filter, rating_query]
        if country_filter:
            conditions.append(country_filter)

        full_query = {"$and": conditions}

        for player in db[PLAYERS].find(full_query).limit(quota):
            picked.append(player)
            seen_ids.add(player["_id"])

    # Fill any shortfall with the most recently ingested remaining players
    shortfall = limit - len(picked)
    if shortfall > 0:
        fallback_conditions = [
            unprocessed_filter,
            {"_id": {"$nin": list(seen_ids)}}
        ]
        if country_filter:
            fallback_conditions.append(country_filter)

        fallback_query = {"$and": fallback_conditions}

        fallback_players = (
            db[PLAYERS]
            .find(fallback_query)
            .sort("fetched_at", -1)
            .limit(shortfall)
        )
        picked.extend(fallback_players)

    # Shuffle so a partially evaluated run is not biased towards the lowest rating bands
    random.shuffle(picked)

    return picked


# Sends up to MAX_GAMES_PER_PLAYER filtered games per selected player from one archive month
def main() -> int:
    # CLI: player limit, optional country, optional archive month (YYYY/MM)
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    country = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "None" else None

    month = ARCHIVE_MONTH
    if len(sys.argv) > 3 and sys.argv[3] != "None" and not sys.argv[3].startswith("--"):
        month = sys.argv[3]

    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc)
    db = mongo_db(DATABASE)

    players = select_players(db, country, limit)
    if not players:
        print("No players matched query criteria.")
        return 1

    fetcher = make_fetcher(DELAY)
    producer = make_producer(BOOTSTRAP)
    games_sent = 0

    for player in players:
        username = player.get("username") or player.get("_id")
        sent_for_player = 0

        # One request per player for the fixed month; a month with no games returns None
        url = f"{BASE}/player/{username}/games/{month}"

        if archive := fetcher.get(url):
            games = archive.get("games", [])

            # Newest games first, keeping those that pass the filter up to the cap
            for game in reversed(games):
                if sent_for_player >= MAX_GAMES_PER_PLAYER:
                    break

                if game_reject_reason(game):
                    continue

                producer.send(
                    TOPIC_GAMES,
                    key=str(username),
                    value=envelope(run_id, url, game),
                )
                sent_for_player += 1
                games_sent += 1

        # Mark the player as collected so re-runs skip them, even when they had no games
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        db[PLAYERS].update_one(
            {"_id": player["_id"]},
            {"$set": {"games_through": current_month}}
        )

    producer.flush()
    producer.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nCompleted: {games_sent} individual games sent | {fetcher.requests_made} HTTP reqs | Time: {elapsed/60:.2f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())