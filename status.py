#!/usr/bin/env python3
"""Where the run has got to, at a glance.

    docker compose exec python python -m status
    docker compose exec python python -m status --watch     # redraw every 60s

Reads Mongo only -- safe to run as often as you like while the pipeline is
working, and it touches nothing.

Targets come from the planned scope and are only used to draw the bars; set them
with PLAN_PLAYERS / PLAN_ANALYSED / PLAN_GAMES_PER_PLAYER if the scope changes.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from ingestion.common import mongo_db

DATABASE = "chess_db"

# The planned run: 1000 players per core-Europe country, of whom 200 per country
# get their games analysed, at up to 20 games each.
PLAN_PLAYERS = int(os.getenv("PLAN_PLAYERS", 44127))
PLAN_ANALYSED = int(os.getenv("PLAN_ANALYSED", 9000))
PLAN_GAMES_PER_PLAYER = float(os.getenv("PLAN_GAMES_PER_PLAYER", 12))


def bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "-" * width
    filled = min(width, int(width * done / total))
    return "#" * filled + "." * (width - filled)


def humanise(hours: float) -> str:
    if hours <= 0:
        return "--"
    if hours < 1:
        return f"{hours * 60:.0f} min"
    if hours < 48:
        return f"{hours:.1f} h"
    return f"{hours / 24:.1f} days"


def spark_jobs() -> str:
    """Which Spark applications are alive, by their script name."""
    try:
        out = subprocess.run(
            ["ps", "-eo", "args"], capture_output=True, text=True, timeout=10
        ).stdout
    except Exception:
        return "unknown"

    running = [
        name
        for name, needle in (("processor", "processor.py"), ("evaluator", "evaluator.py"))
        if any(needle in line and "SparkSubmit" in line for line in out.splitlines())
    ]
    return ", ".join(running) if running else "NONE RUNNING"


def snapshot(db) -> dict:
    players = db.chess_players.estimated_document_count()
    games = db.chess_games.estimated_document_count()
    move_docs = db.chess_move_evals.estimated_document_count()

    # Exactly one document per game carries ply 1, so counting those is an exact
    # game count without an expensive distinct() over millions of move rows.
    evaluated = db.chess_move_evals.count_documents({"ply": 1, "engine": {"$exists": True}})

    hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    recent = db.chess_move_evals.count_documents({"ply": 1, "evaluated_at": {"$gt": hour_ago}})

    return {
        "players": players,
        "games": games,
        "move_docs": move_docs,
        "evaluated": evaluated,
        "recent": recent,
    }


def render(db) -> None:
    s = snapshot(db)
    target_games = int(PLAN_ANALYSED * PLAN_GAMES_PER_PLAYER)
    remaining = max(0, target_games - s["evaluated"])
    rate = s["recent"]  # games per hour, measured over the last hour

    print(f"  CHESS PIPELINE   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  spark jobs: {spark_jobs()}")
    print()
    print(f"  players collected   {s['players']:>9,} / {PLAN_PLAYERS:<9,} [{bar(s['players'], PLAN_PLAYERS)}] {s['players']/PLAN_PLAYERS*100:5.1f}%")
    print(f"  games collected     {s['games']:>9,} / {target_games:<9,} [{bar(s['games'], target_games)}] {s['games']/max(target_games,1)*100:5.1f}%")
    print(f"  games EVALUATED     {s['evaluated']:>9,} / {target_games:<9,} [{bar(s['evaluated'], target_games)}] {s['evaluated']/max(target_games,1)*100:5.1f}%")
    print()
    print(f"  move documents      {s['move_docs']:>9,}")
    print(f"  engine throughput   {rate:>9,} games in the last hour ({rate/60:.2f}/min)")

    if rate > 0 and remaining > 0:
        print(f"  estimated remaining {humanise(remaining / rate):>9}   ({remaining:,} games to go)")
    elif remaining == 0 and s["evaluated"]:
        print("  estimated remaining        --   target reached")
    else:
        print("  estimated remaining        --   no games finished in the last hour yet")

    # A game whose moves are in Mongo but whose rank is not is normal while the
    # engine is behind; a game the engine has passed that still has no moves
    # would mean the processor is lagging or down.
    backlog = s["games"] - s["evaluated"]
    if backlog > 0:
        print(f"  engine backlog      {backlog:>9,} games collected but not yet evaluated")


def main() -> int:
    db = mongo_db(DATABASE)
    watch = "--watch" in sys.argv

    while True:
        if watch:
            print("\033[2J\033[H", end="")  # clear screen, home cursor
        render(db)
        if not watch:
            return 0
        print("\n  (--watch: refreshing every 60s, ctrl-c to stop)")
        time.sleep(60)


if __name__ == "__main__":
    sys.exit(main())
