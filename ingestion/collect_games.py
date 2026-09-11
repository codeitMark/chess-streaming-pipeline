from datetime import datetime, timezone
import os
import sys
import re
from typing import Any
import uuid

from ingestion.common import BASE, TOPIC_GAMES, envelope, make_fetcher, make_producer, mongo_db

DATABASE = "chess_db"
PLAYERS = "chess_players"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = 1.0
RATING_BANDS = ((0, 800), (800, 1200), (1200, 1600), (1600, 2000), (2000, 5000))


def get_month_from_url(url: str) -> str:
    """Extracts 'YYYY-MM' from a Chess.com archive URL."""
    parts = url.rstrip("/").split("/")
    return f"{parts[-2]}-{parts[-1]}"


def select_players(db: Any, country: str | None, limit: int) -> list[dict]:
    """Selects players evenly across rating bands without phantom flags."""
    match: dict[str, Any] = {}
    if country:
        country_code = country.upper().strip()

        country_regex = re.compile(rf"(?:/|^){country_code}$", re.IGNORECASE)
        match["$or"] = [
            {"country": country_regex},
            {"profile.country": country_regex}
        ]

    per_band = max(1, limit // len(RATING_BANDS))
    picked: list[dict] = []
    seen_ids: set[Any] = set()

    for low, high in RATING_BANDS:
        if len(picked) >= limit:
            break

        quota = min(per_band, limit - len(picked))
        
    # Check both flat rating fields and nested API paths
        rating_query = {
            "$or": [
                {"blitz_rating": {"$gte": low, "$lt": high}},
                {"blitz.rating": {"$gte": low, "$lt": high}},
                {"stats.chess_blitz.last.rating": {"$gte": low, "$lt": high}}
            ]
        }
        
        # Combine country match (if present) with rating bounds
        if "$or" in match:
            full_query = {"$and": [match, rating_query]}
        else:
            full_query = {**match, **rating_query}
        
        for player in db[PLAYERS].find(full_query).limit(quota):
            picked.append(player)
            seen_ids.add(player["_id"])

    # Fallback to remaining players sorted by last_online if limit is not reached
    shortfall = limit - len(picked)
    if shortfall > 0:
        if "$or" in match:
            fallback_query = {"$and": [match, {"_id": {"$nin": list(seen_ids)}}]}
        else:
            fallback_query = {**match, "_id": {"$nin": list(seen_ids)}}
            
        fallback_players = (
            db[PLAYERS]
            .find(fallback_query)
            .sort("last_online", -1)
            .limit(shortfall)
        )
        picked.extend(fallback_players)

    return picked


def filter_months(months: list[str], since: str | None, games_through: str | None) -> list[str]:
    """Filters archive months based on lower bounds."""
    if since:
        months = [m for m in months if get_month_from_url(m) >= since]
    if games_through:
        months = [m for m in months if get_month_from_url(m) > games_through]
    return months


def main() -> int:
    # Parse CLI arguments: python collect_games.py <limit> <country> <since_YYYY-MM> [--resume]
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    country = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "None" else None
    since = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != "None" else None
    resume = "--resume" in sys.argv

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

    for i, player in enumerate(players, 1):
        username = player.get("profile", {}).get("username") or player.get("username") or player.get("_id")
        
        # Pull player archive index
        res = fetcher.get(f"{BASE}/player/{username}/games/archives") or {}
        months = res.get("archives", [])
        
        games_through = player.get("games_through") if resume else None
        months = filter_months(months, since, games_through)

        for url in months:
            pgn_url = f"{url.rstrip('/')}/pgn"

            if archive := fetcher.get(url):
                games = archive.get("games", [])
                
                # Stream individual game items directly to Kafka
                for game in games:
                    producer.send(
                        TOPIC_GAMES,
                        key=str(username),
                        value=envelope(run_id, url, game),
                    )
                    games_sent += 1

        # Track progress checkpoint back to Mongo
        if months:
            db[PLAYERS].update_one(
                {"_id": player["_id"]}, 
                {"$set": {"games_through": get_month_from_url(months[-1])}}
            )
        
        print(f"  {i:>4}/{len(players)} | Player: {username:<20} | Processed Months: {len(months):>2}")

    producer.flush()
    producer.close()
    
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nCompleted: {games_sent} individual games sent | {fetcher.requests_made} HTTP reqs | Time: {elapsed/60:.2f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())