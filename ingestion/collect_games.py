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
    """Selects players evenly across rating bands who need game collection."""
    current_month = datetime.now(timezone.utc).strftime("%Y-%m") # If players haven't been fetched for the current month. strftime to format it to YYYY-mm
    unprocessed_filter = {
        "$or": [
            {"games_through": {"$exists": False}},
            {"games_through": {"$ne": current_month}}
        ]
    }

    country_filter = {} # country_filter if specified in argument
    if country:
        country_filter = {"country_code": country.upper().strip()}

    per_band = max(1, limit // len(RATING_BANDS))
    picked: list[dict] = []
    seen_ids: set[Any] = set()

    # Fetches players evenly across rating bands
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

    # 4. If rating bands didn't fill the total limit, fill with other players.
    shortfall = limit - len(picked)
    if shortfall > 0:
        fallback_conditions = [
            unprocessed_filter,
            {"_id": {"$nin": list(seen_ids)}} # excludes already picked players
        ]
        if country_filter:
            fallback_conditions.append(country_filter)

        fallback_query = {"$and": fallback_conditions}
            
        fallback_players = (
            db[PLAYERS]
            .find(fallback_query)
            .sort("fetched_at", -1)  # Most recently ingested players are added first to fill the list (regardless of rating)
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
        username = player.get("username") or player.get("_id")
        
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
            current_month = datetime.now(timezone.utc).strftime("%Y-%m")
            db[PLAYERS].update_one(
                {"_id": player["_id"]}, 
                {"$set": {"games_through": current_month}} # changed to current_month instead of actual month the last game is from. This is to avoid it being added to the potential pull pool because they didnt play for the current month.
            )
        
        print(f"  {i:>4}/{len(players)} | Player: {username:<20} | Processed Months: {len(months):>2}")

    producer.flush()
    producer.close()
    
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nCompleted: {games_sent} individual games sent | {fetcher.requests_made} HTTP reqs | Time: {elapsed/60:.2f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())