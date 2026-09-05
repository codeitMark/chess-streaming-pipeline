#!/usr/bin/env python3
"""Chess.com collector -- pulls player snapshots and games, drops every field we
do not need, and writes one JSON file per intended MongoDB collection.

The output directory is a rough stand-in for the database: each file is a
collection, each object in it is a document with the `_id` it would carry in
Mongo. It is not a real schema, but it makes the document model concrete enough
to argue about before anything is written to Atlas.

In the finished pipeline this work is split -- the collector produces raw
payloads to Kafka, and the parse-and-validate consumer writes typed documents to
Mongo. This script collapses both halves so we can see clean data now. Records
are still upserted by `_id`, so re-running is idempotent in the same way
replaying a Kafka offset range would be.

Collection is strictly SERIAL -- see probe_api.py for why that is not
negotiable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE = "https://api.chess.com/pub"
DEFAULT_CONTACT = "group12@example.invalid"

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30

# Only these three matter for our questions. Daily is dropped, which also means
# time_per_move and timeout_percent leave the project entirely -- they exist
# only inside chess_daily.record.
LIVE_CLASSES = ("blitz", "rapid", "bullet")

# What each output file is meant to become. Written to _collections.json so the
# intended grain, key and indexes travel with the data.
COLLECTIONS: dict[str, dict[str, Any]] = {
    "country_rosters": {
        "grain": "one doc per country per day",
        "key": "(country, snapshot_date)",
        "indexes": ["(country, snapshot_date)"],
        "note": "Sampling frame for Q2 growth and churn. Not in the architecture "
                "doc's collection table, but the day-over-day diff of these "
                "username lists is the only churn signal available.",
    },
    "player_snapshots": {
        "grain": "one doc per player per day",
        "key": "(player_id, snapshot_date)",
        "indexes": ["(player_id, snapshot_date)", "(country, snapshot_date)"],
        "note": "Profile and stats flattened into a single dated document.",
    },
    "games": {
        "grain": "one doc per game, movetext and clocks embedded",
        "key": "game_url",
        "indexes": ["game_url", "(time_class, end_time)"],
        "note": "Deduplicated on URL -- both players' archives contain the same game.",
    },
    "rejects": {
        "grain": "one doc per record that failed a filter",
        "key": "(run_id, game_url)",
        "indexes": ["reason"],
        "note": "Nothing is dropped silently. Reason codes make the funnel auditable.",
    },
    "collector_state": {
        "grain": "one doc per fetched URL",
        "key": "url",
        "indexes": ["url"],
        "note": "Last ETag per URL so the next run can send If-None-Match and "
                "get 304s. Pass this file back in with --state.",
    },
}


# --------------------------------------------------------------------------
# Sink
# --------------------------------------------------------------------------


class MongoLikeSink:
    """Buffers documents per collection and writes one JSON array per file.

    Documents are held in a dict keyed by `_id`, so a repeated write replaces
    the earlier one -- the same idempotent-upsert semantics the real consumer
    needs in order to make Kafka replays safe.
    """

    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {name: {} for name in COLLECTIONS}
        self.upserts: Counter[str] = Counter()
        self.replaced: Counter[str] = Counter()

    def upsert(self, collection: str, doc: dict[str, Any]) -> bool:
        """Returns True if this was a new document, False if it replaced one."""
        store = self.docs[collection]
        is_new = doc["_id"] not in store
        if not is_new:
            self.replaced[collection] += 1
        store[doc["_id"]] = doc
        self.upserts[collection] += 1
        return is_new

    def counts(self) -> dict[str, int]:
        return {name: len(docs) for name, docs in self.docs.items() if docs}

    def write(self, out_dir: Path) -> dict[str, int]:
        out_dir.mkdir(parents=True, exist_ok=True)
        sizes: dict[str, int] = {}
        for name, docs in self.docs.items():
            if not docs:
                continue
            path = out_dir / f"{name}.json"
            body = json.dumps(list(docs.values()), indent=2, ensure_ascii=False)
            path.write_text(body, encoding="utf-8")
            sizes[name] = len(body.encode())
        return sizes


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


def build_session(contact: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": f"CS4010-USN-Group12/0.1 (chess pipeline coursework; contact: {contact})",
            "Accept": "application/json",
        }
    )
    return session


class Fetcher:
    """Serial, rate-limited GET with ETag support and 429 back-off."""

    def __init__(self, session: requests.Session, delay: float, run_id: str) -> None:
        self.session = session
        self.delay = delay
        self.run_id = run_id
        self.raw_bytes = 0
        self.requests_made = 0
        self.not_modified = 0
        self.etags: dict[str, str] = {}
        self.state: dict[str, dict[str, Any]] = {}

    def load_state(self, path: Path) -> int:
        """Restore ETags from a previous run's collector_state.json."""
        docs = json.loads(path.read_text(encoding="utf-8"))
        for doc in docs:
            if doc.get("etag"):
                self.etags[doc["_id"]] = doc["etag"]
        return len(self.etags)

    def get(self, url: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        time.sleep(self.delay)

        headers: dict[str, str] = {}
        if url in self.etags:
            headers["If-None-Match"] = self.etags[url]

        response = self.session.get(
            url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if response.status_code == 429:
            wait = float(response.headers.get("Retry-After", 5))
            print(f"    ! 429, backing off {wait}s")
            time.sleep(wait)
            response = self.session.get(
                url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )

        self.requests_made += 1
        self.raw_bytes += len(response.content)
        fetched_at = datetime.now(timezone.utc).isoformat()
        etag = response.headers.get("ETag")

        if etag:
            self.etags[url] = etag
        self.state[url] = {
            "_id": url,
            "etag": etag,
            "last_status": response.status_code,
            "last_fetched": fetched_at,
            "run_id": self.run_id,
        }

        envelope = {
            "run_id": self.run_id,
            "source_url": url,
            "fetched_at": fetched_at,
            "http_status": response.status_code,
            "etag": etag,
        }

        if response.status_code == 304:
            self.not_modified += 1
            return None, envelope
        if response.status_code != 200:
            return None, envelope

        try:
            return response.json(), envelope
        except ValueError:
            return None, envelope


# --------------------------------------------------------------------------
# Projection -- the field filtering
# --------------------------------------------------------------------------


def unmapped(payload: dict[str, Any], known: set[str]) -> dict[str, Any]:
    """Everything the projection did not recognise.

    Chess.com adds fields over time; `start_time` and `match` already showed up
    in game records without being documented. Capturing them costs a few bytes
    and prevents silent loss.
    """
    return {k: v for k, v in payload.items() if k not in known}


def iso_from_country_url(url: str | None) -> str | None:
    """'https://api.chess.com/pub/country/NO' -> 'NO'."""
    return url.rsplit("/", 1)[-1] if url else None


def username_from_id(player_ref: dict[str, Any]) -> str | None:
    """Game records carry '@id' rather than a username field.

    The numeric player_id only appears on the profile, and the profile has no
    uuid -- so the practical join key between games and snapshots is the
    lowercase username in '@id'. The player uuid is kept alongside it because it
    survives username changes, which the username does not.
    """
    ref = player_ref.get("@id")
    return ref.rsplit("/", 1)[-1] if ref else None


PROFILE_KNOWN = {
    "player_id", "username", "country", "joined", "last_online", "status",
    "title", "avatar", "@id", "url", "followers", "is_streamer", "verified",
    "league", "streaming_platforms", "name", "location", "twitch_url", "fide",
}

STATS_KNOWN = {
    "chess_blitz", "chess_rapid", "chess_bullet", "chess_daily", "tactics",
    "puzzle_rush", "fide", "lessons", "puzzle_rush_wr", "chess960_daily",
}

GAME_KNOWN = {
    "url", "pgn", "time_control", "end_time", "start_time", "rated", "tcn",
    "uuid", "initial_setup", "fen", "time_class", "rules", "white", "black",
    "eco", "match", "accuracies", "tournament",
}


def project_time_class(block: dict[str, Any]) -> dict[str, Any]:
    last = block.get("last") or {}
    best = block.get("best") or {}
    record = block.get("record") or {}
    out = {
        "rating": last.get("rating"),
        "rd": last.get("rd"),
        "date": last.get("date"),
        "win": record.get("win"),
        "loss": record.get("loss"),
        "draw": record.get("draw"),
    }
    # `best` is missing on time classes the player has barely touched, so this
    # has to be conditional even when the time class itself is present.
    if best.get("rating") is not None:
        out["best_rating"] = best["rating"]
        out["best_date"] = best.get("date")
    return out


def build_snapshot(
    profile: dict[str, Any],
    stats: dict[str, Any] | None,
    snapshot_date: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """One document per player per day -- profile and stats flattened together."""
    stats = stats or {}

    doc: dict[str, Any] = {
        "_id": f"{profile.get('player_id')}_{snapshot_date}",
        "player_id": profile.get("player_id"),
        "username": profile.get("username"),
        "snapshot_date": snapshot_date,
        "country": iso_from_country_url(profile.get("country")),
        "joined": profile.get("joined"),
        "last_online": profile.get("last_online"),
        "status": profile.get("status"),
    }
    # Absent for untitled players -- omit rather than store null, so "no title"
    # and "we did not look" stay distinguishable.
    if profile.get("title"):
        doc["title"] = profile["title"]

    ratings: dict[str, Any] = {}
    for cls in LIVE_CLASSES:
        block = stats.get(f"chess_{cls}")
        # Omitted entirely when the player has never played the class. Never
        # impute a rating -- absent is not zero.
        if block:
            ratings[cls] = project_time_class(block)
    if ratings:
        doc["ratings"] = ratings

    tactics = stats.get("tactics") or {}
    if tactics:
        # No current tactics rating is exposed, only the extremes.
        doc["tactics"] = {
            "highest_rating": (tactics.get("highest") or {}).get("rating"),
            "highest_date": (tactics.get("highest") or {}).get("date"),
            "lowest_rating": (tactics.get("lowest") or {}).get("rating"),
            "lowest_date": (tactics.get("lowest") or {}).get("date"),
        }

    rush = (stats.get("puzzle_rush") or {}).get("best") or {}
    if rush:
        doc["puzzle_rush"] = {
            "score": rush.get("score"),
            "total_attempts": rush.get("total_attempts"),
        }

    # 0 means "no FIDE rating", not a rating of zero.
    doc["fide"] = stats.get("fide") or None

    extra = {**unmapped(profile, PROFILE_KNOWN), **unmapped(stats, STATS_KNOWN)}
    if extra:
        doc["_unmapped"] = extra

    doc["_ingest"] = envelope
    return doc


def movetext_from_pgn(pgn: str | None) -> str:
    """Strip the PGN header block, keep the moves and their [%clk] annotations.

    Every header duplicates a structured field we already keep, and the block is
    roughly a fifth of the pgn field.
    """
    if not pgn:
        return ""
    return pgn.split("\n\n", 1)[1].strip() if "\n\n" in pgn else ""


def build_game(payload: dict[str, Any], envelope: dict[str, Any]) -> dict[str, Any]:
    movetext = movetext_from_pgn(payload.get("pgn"))
    doc: dict[str, Any] = {
        "_id": payload.get("url"),
        "url": payload.get("url"),
        "end_time": payload.get("end_time"),
        "time_class": payload.get("time_class"),
        "time_control": payload.get("time_control"),
        "eco": payload.get("eco"),
    }

    for side in ("white", "black"):
        ref = payload.get(side) or {}
        doc[side] = {
            "username": username_from_id(ref),
            "uuid": ref.get("uuid"),
            "rating": ref.get("rating"),
            "result": ref.get("result"),
        }

    doc["movetext"] = movetext
    doc["has_clk"] = "[%clk" in movetext

    # Chess.com's own accuracy, present only when someone ran a game review.
    # Proprietary, so it cannot be a target -- but it validates our own
    # centipawn-loss pipeline on the overlap.
    if payload.get("accuracies"):
        doc["accuracies"] = payload["accuracies"]

    extra = unmapped(payload, GAME_KNOWN)
    if extra:
        doc["_unmapped"] = extra

    doc["_ingest"] = envelope
    return doc


def build_reject(
    payload: dict[str, Any], reason: str, run_id: str, source_url: str
) -> dict[str, Any]:
    """Nothing is dropped silently -- §3 wants a reason code for every loss."""
    return {
        "_id": f"{run_id}:{payload.get('url')}",
        "reason": reason,
        "game_url": payload.get("url"),
        "time_class": payload.get("time_class"),
        "rules": payload.get("rules"),
        "rated": payload.get("rated"),
        "source_url": source_url,
        "run_id": run_id,
        "rejected_at": datetime.now(timezone.utc).isoformat(),
    }


def game_reject_reason(game: dict[str, Any]) -> str | None:
    """None if the game should be kept, otherwise why it was dropped."""
    if game.get("rules") != "chess":
        return f"variant:{game.get('rules')}"
    if game.get("time_class") not in LIVE_CLASSES:
        return f"time_class:{game.get('time_class')}"
    if not game.get("rated"):
        return "unrated"
    return None


# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect and filter Chess.com data into MongoDB-shaped JSON."
    )
    parser.add_argument(
        "--countries", default="NO",
        help="Comma-separated ISO codes, e.g. NO,IS,DK (default: NO)",
    )
    parser.add_argument(
        "--players", type=int, default=25,
        help="Players per country. The endpoint caps at 10000 and ignores all "
             "pagination parameters (default: 25)",
    )
    parser.add_argument(
        "--months", type=int, default=1,
        help="Most recent monthly archives to pull per player (default: 1)",
    )
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument(
        "--contact", default=os.environ.get("CHESS_API_CONTACT", DEFAULT_CONTACT)
    )
    parser.add_argument(
        "--out", default=None,
        help="Output directory (default: ingestion/out/<timestamp>_<run_id>/)",
    )
    parser.add_argument(
        "--state", default=None,
        help="A previous run's collector_state.json. Restores ETags so unchanged "
             "resources come back as 304 instead of a full payload.",
    )
    parser.add_argument(
        "--no-games", action="store_true",
        help="Snapshots only. Much faster -- two requests per player.",
    )
    args = parser.parse_args()

    if args.contact == DEFAULT_CONTACT:
        print("WARNING: no contact address set. Set CHESS_API_CONTACT.\n")

    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc)
    snapshot_date = started.date().isoformat()
    out_dir = Path(
        args.out
        or Path(__file__).parent / "out" / f"{started:%Y%m%dT%H%M%SZ}_{run_id}"
    )

    fetcher = Fetcher(build_session(args.contact), args.delay, run_id)
    if args.state:
        restored = fetcher.load_state(Path(args.state))
        print(f"restored {restored} ETags from {args.state}")

    sink = MongoLikeSink()
    rejects: Counter[str] = Counter()
    duplicates = 0
    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]

    print(f"run {run_id} | countries={','.join(countries)} | "
          f"{args.players} players each | delay {args.delay}s\n")

    for iso in countries:
        listing, env = fetcher.get(f"{BASE}/country/{iso}/players")
        if not listing:
            print(f"[{iso}] player list unavailable, skipping")
            continue

        roster = listing.get("players", [])
        sink.upsert("country_rosters", {
            "_id": f"{iso}_{snapshot_date}",
            "country": iso,
            "snapshot_date": snapshot_date,
            "player_count": len(roster),
            # The endpoint truncates at 10000 alphabetically and says so in a
            # comment field. Worth storing -- it says "temporarily", so it may
            # change under us.
            "capped": bool(listing.get("comment")),
            "api_comment": listing.get("comment"),
            "players": roster,
            "_ingest": env,
        })

        usernames = roster[: args.players]
        print(f"[{iso}] roster {len(roster)}, processing {len(usernames)}")

        for i, username in enumerate(usernames, 1):
            profile, env_p = fetcher.get(f"{BASE}/player/{username}")
            if not profile:
                continue

            stats, _ = fetcher.get(f"{BASE}/player/{username}/stats")
            sink.upsert(
                "player_snapshots",
                build_snapshot(profile, stats, snapshot_date, env_p),
            )

            if args.no_games:
                if i % 10 == 0:
                    print(f"  {i}/{len(usernames)} snapshots")
                continue

            archives, _ = fetcher.get(f"{BASE}/player/{username}/games/archives")
            archive_urls = (archives or {}).get("archives", [])[-args.months:]

            kept_here = 0
            for archive_url in archive_urls:
                month, env_g = fetcher.get(archive_url)
                for game in (month or {}).get("games", []):
                    reason = game_reject_reason(game)
                    if reason:
                        rejects[reason] += 1
                        sink.upsert(
                            "rejects",
                            build_reject(game, reason, run_id, archive_url),
                        )
                        continue
                    # Both players' archives contain the same game. This is
                    # expected overlap, not a validation failure, so it is
                    # counted separately rather than sent to rejects.
                    if not sink.upsert("games", build_game(game, env_g)):
                        duplicates += 1
                    else:
                        kept_here += 1

            print(f"  {i:>3}/{len(usernames)} {username:<22} {kept_here:>4} games")

    for doc in fetcher.state.values():
        sink.upsert("collector_state", doc)

    sizes = sink.write(out_dir)

    summary = {
        "requests_made": fetcher.requests_made,
        "not_modified_304": fetcher.not_modified,
        "raw_bytes_downloaded": fetcher.raw_bytes,
        "bytes_kept": sum(sizes.values()),
        "documents": sink.counts(),
        "file_bytes": sizes,
        "rejected": dict(rejects),
        "duplicates_deduplicated": duplicates,
    }

    (out_dir / "_run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "started_at": started.isoformat(),
                "config": {
                    "countries": countries,
                    "players_per_country": args.players,
                    "months": args.months,
                    "delay_seconds": args.delay,
                    "live_classes": list(LIVE_CLASSES),
                },
                "summary": summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (out_dir / "_collections.json").write_text(
        json.dumps(COLLECTIONS, indent=2), encoding="utf-8"
    )

    print(f"\nrequests {fetcher.requests_made}  |  "
          f"downloaded {fetcher.raw_bytes:,} B  |  "
          f"kept {sum(sizes.values()):,} B")
    for name, count in sink.counts().items():
        print(f"  {name:<20} {count:>6} docs   {sizes.get(name, 0):>10,} B")
    if rejects:
        print(f"rejected   {dict(rejects)}")
    if duplicates:
        print(f"duplicates {duplicates} (deduplicated on game url)")
    print(f"\nWritten to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
