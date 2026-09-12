from datetime import datetime, timezone
import os
import random
import sys
import re
from typing import Any
import uuid

from ingestion.common import BASE, TOPIC_GAMES, envelope, make_fetcher, make_producer, mongo_db

DATABASE = "chess_db"
PLAYERS = "chess_players"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = 0.6 # Chess.com rate-limits parallel requests, not serial ones. We stay serial and keep a smaller courtesy gap; 0.6s is the floor we are willing to run at.
RATING_BANDS = ((0, 800), (800, 1200), (1200, 1600), (1600, 2000), (2000, 5000))
LIVE_CLASSES = ("blitz", "rapid", "bullet")
MAX_GAMES_PER_PLAYER = 20
ARCHIVE_MONTH = "2026/08" # One fixed month for everyone, as YYYY/MM. Whoever has fewer than MAX_GAMES_PER_PLAYER games in it contributes fewer; we do not look further back.


def game_reject_reason(game: dict[str, Any]) -> str | None:
    if game.get("rules") != "chess":
        return f"variant:{game.get('rules')}"

    if game.get("time_class") not in LIVE_CLASSES:
        return f"time_class:{game.get('time_class')}"

    if not game.get("rated"):
        return "unrated"

    return None


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

    # The bands above are filled in order, so picked comes out sorted by rating:
    # every 0-800 player, then every 800-1200, and so on. Kafka preserves that
    # order and evaluator.py consumes it in order, so an evaluation run that does
    # not reach the end of the topic -- the normal case, since Stockfish is far
    # slower than collection -- would cover only the weakest players, and any
    # blunder rate measured off it would be confounded with rating. Shuffling
    # makes any prefix of the run an unbiased sample across the bands.
    random.shuffle(picked)

    return picked


def main() -> int:
    # Parse CLI arguments: python collect_games.py <limit> <country> <month_YYYY/MM>
    # Re-running is always resumable: select_players skips anyone whose
    # games_through is already this run's month, so there is no --resume flag.
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

    for i, player in enumerate(players, 1):
        username = player.get("username") or player.get("_id")
        sent_for_player = 0

        # One request per player, straight at the month we want. Asking the
        # archive index which months exist costs a request per player and only
        # tells us things we no longer act on, now that everybody is sampled from
        # the same fixed month. A player with no games that month 404s, which the
        # fetcher returns as None.
        url = f"{BASE}/player/{username}/games/{month}"

        if archive := fetcher.get(url):
            games = archive.get("games", [])

            # A month runs oldest -> newest, so take from the end: the cap keeps
            # each player's most recent games rather than an arbitrary 20.
            for game in reversed(games):
                if sent_for_player >= MAX_GAMES_PER_PLAYER:
                    break

                # Filtered here rather than in each consumer. An archive is
                # whatever the player happened to play, and anything outside
                # standard blitz/rapid/bullet costs broker bandwidth, Mongo
                # documents and -- in evaluator.py -- minutes of Stockfish
                # per game. Variants are the sharp case: Stockfish reads a
                # Chess960 game as standard chess and returns confident
                # nonsense instead of an error, so one that reaches the topic
                # becomes wrong data rather than missing data.
                if game_reject_reason(game):
                    continue

                producer.send(
                    TOPIC_GAMES,
                    key=str(username),
                    value=envelope(run_id, url, game),
                )
                sent_for_player += 1
                games_sent += 1

        # Track progress checkpoint back to Mongo. Set even when the player had
        # no games in ARCHIVE_MONTH -- they have been looked at, and without this
        # every re-run would spend a request on the same empty accounts again.
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        db[PLAYERS].update_one(
            {"_id": player["_id"]},
            {"$set": {"games_through": current_month}} # the run's own month, not the archive month: it marks "already collected", so select_players stops picking them.
        )


        print(f"  {i:>4}/{len(players)} | Player: {username:<20} | Games sent: {sent_for_player:>2}/{MAX_GAMES_PER_PLAYER}")

    producer.flush()
    producer.close()
    
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nCompleted: {games_sent} individual games sent | {fetcher.requests_made} HTTP reqs | Time: {elapsed/60:.2f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())