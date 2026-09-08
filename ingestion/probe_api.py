#!/usr/bin/env python3
"""Connection and endpoint probe for the Chess.com Public API.

Purpose: confirm that every endpoint the pipeline depends on is reachable,
returns the shape we expect, and supports conditional requests. Writes a single
JSON report describing what came back.

This is a diagnostic tool, not part of the pipeline. It does not write to Kafka
or MongoDB. Nothing here is meant to be reused by the collector as-is.

Two rules from the architecture doc are enforced here because they are easy to
break later by accident:

1. Collection is strictly SERIAL. Chess.com states that serial access is
   unlimited, but that parallel requests may return 429 and that abnormal
   activity can get an application blocked entirely. Do not add threads, do not
   add asyncio, and do not shard this across machines. A block would end the
   project.

2. Every request carries a User-Agent with a contact address, and repeat
   requests use ETag / If-None-Match so unchanged resources return 304 instead
   of a full payload. The 304 path is probed explicitly below, because the whole
   collector design assumes it works.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE = "https://api.chess.com/pub"

# Chess.com asks for a contact address in the User-Agent so they can reach us
# before blocking us. Set CHESS_API_CONTACT in your environment (or .env) --
# the placeholder below is deliberately invalid so an unset value is obvious.
DEFAULT_CONTACT = "group12@example.invalid"

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30


def build_session(contact: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": f"CS4010-USN-Group12/0.1 (chess pipeline coursework; contact: {contact})",
            "Accept": "application/json",
        }
    )
    return session


def shrink(value: Any, max_items: int = 3, max_chars: int = 400) -> Any:
    """Bound a payload so the report stays readable.

    A single monthly archive can be several megabytes. We want to see the shape
    of the response, not every record in it.
    """
    if isinstance(value, dict):
        return {k: shrink(v, max_items, max_chars) for k, v in value.items()}
    if isinstance(value, list):
        out = [shrink(v, max_items, max_chars) for v in value[:max_items]]
        if len(value) > max_items:
            out.append(f"<... {len(value) - max_items} more items omitted>")
        return out
    if isinstance(value, str) and len(value) > max_chars:
        return value[:max_chars] + f"<... truncated, {len(value)} chars total>"
    return value


def fetch(
    session: requests.Session,
    name: str,
    url: str,
    delay: float,
    etag: str | None = None,
) -> dict[str, Any]:
    """Perform one serial GET and describe the result.

    Never raises on an HTTP error: a failing endpoint is a result we want in the
    report, not a crash. The delay happens before the request so that every call
    in a run is spaced, including the first one after a retry.
    """
    time.sleep(delay)

    headers: dict[str, str] = {}
    if etag:
        headers["If-None-Match"] = etag

    record: dict[str, Any] = {
        "name": name,
        "url": url,
        "conditional": bool(etag),
        "status": None,
        "ok": False,
        "elapsed_ms": None,
        "bytes": None,
        "etag": None,
        "error": None,
        "sample": None,
    }

    started = time.perf_counter()
    try:
        response = session.get(
            url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )

        # Serial access should never hit this, but if it does we want to see it
        # in the report rather than silently treat 429 as a failed endpoint.
        if response.status_code == 429:
            wait = float(response.headers.get("Retry-After", 5))
            print(f"  ! 429 on {name}, waiting {wait}s and retrying once")
            time.sleep(wait)
            response = session.get(
                url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )

        record["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
        record["status"] = response.status_code
        record["bytes"] = len(response.content)
        record["etag"] = response.headers.get("ETag")
        record["content_type"] = response.headers.get("Content-Type")

        # 304 is a success for our purposes: it proves conditional requests work.
        record["ok"] = response.status_code in (200, 304)

        if response.status_code == 200 and response.content:
            try:
                record["payload"] = response.json()
                record["sample"] = shrink(record["payload"])
            except ValueError as exc:
                record["error"] = f"response was not valid JSON: {exc}"
                record["ok"] = False

    except requests.RequestException as exc:
        record["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
        record["error"] = f"{type(exc).__name__}: {exc}"

    flag = "ok " if record["ok"] else "FAIL"
    print(
        f"  [{flag}] {name:<28} {str(record['status']):<5} "
        f"{str(record['elapsed_ms']) + 'ms':>10} {str(record['bytes'] or 0) + 'B':>12}"
    )
    if record["error"]:
        print(f"         {record['error']}")

    return record


def analyse_games(games: list[dict[str, Any]]) -> dict[str, Any]:
    """Check the two things Q5 and the engine validation depend on.

    Architecture doc, immediate action 3: confirm [%clk] annotations are present
    in the PGN for the time classes we care about, because Question 5 depends
    entirely on this. The `accuracies` field is chess.com's own precomputed
    accuracy -- where present it is free partial ground truth for validating our
    Stockfish pipeline, so it is worth knowing how often we get it.
    """
    by_class: dict[str, dict[str, int]] = {}

    for game in games:
        time_class = game.get("time_class", "unknown")
        bucket = by_class.setdefault(
            time_class, {"games": 0, "with_clk": 0, "with_accuracies": 0}
        )
        bucket["games"] += 1
        if "[%clk" in (game.get("pgn") or ""):
            bucket["with_clk"] += 1
        if game.get("accuracies"):
            bucket["with_accuracies"] += 1

    total = len(games)
    with_clk = sum(b["with_clk"] for b in by_class.values())
    with_acc = sum(b["with_accuracies"] for b in by_class.values())

    return {
        "total_games": total,
        "with_clk": with_clk,
        "with_clk_pct": round(100 * with_clk / total, 1) if total else 0.0,
        "with_accuracies": with_acc,
        "with_accuracies_pct": round(100 * with_acc / total, 1) if total else 0.0,
        "by_time_class": by_class,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe the Chess.com public API endpoints used by this pipeline."
    )
    parser.add_argument(
        "--country", default="NO", help="ISO country code to sample (default: NO)"
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Pin a specific player instead of discovering one from the country list.",
    )
    parser.add_argument(
        "--candidates",
        type=int,
        default=3,
        help="How many discovered players to try before giving up on finding one "
        "with game archives (default: 3).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to wait between requests. Keep this polite (default: 1.0).",
    )
    parser.add_argument(
        "--contact",
        default=os.environ.get("CHESS_API_CONTACT", DEFAULT_CONTACT),
        help="Contact address for the User-Agent header. "
        "Defaults to $CHESS_API_CONTACT.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output JSON path (default: ingestion/out/api_probe_<timestamp>.json).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Store complete payloads instead of bounded samples. Produces a "
        "much larger file.",
    )
    args = parser.parse_args()

    if args.contact == DEFAULT_CONTACT:
        print(
            "WARNING: no contact address set. Chess.com asks for a real address in "
            "the User-Agent\n         so they can contact us instead of blocking "
            "us. Set CHESS_API_CONTACT.\n"
        )

    started_at = datetime.now(timezone.utc)
    out_path = Path(
        args.out
        or Path(__file__).parent
        / "out"
        / f"api_probe_{started_at:%Y%m%dT%H%M%SZ}.json"
    )

    session = build_session(args.contact)
    probes: list[dict[str, Any]] = []

    print(f"Probing Chess.com API serially, {args.delay}s between requests.\n")

    # 1. Leaderboards -- no parameters, so it isolates plain connectivity.
    probes.append(fetch(session, "leaderboards", f"{BASE}/leaderboards", args.delay))

    # 2. Country player list -- the sample frame for the daily snapshot loop.
    country_probe = fetch(
        session,
        "country players",
        f"{BASE}/country/{args.country}/players",
        args.delay,
    )
    probes.append(country_probe)

    # 3. Titled players -- seed set and elite comparison cohort.
    probes.append(fetch(session, "titled (GM)", f"{BASE}/titled/GM", args.delay))

    # Choose which player to probe with. Prefer an explicit --username so runs
    # are reproducible; otherwise take the first few from the country list.
    country_players: list[str] = (
        country_probe.get("payload", {}).get("players", []) if country_probe["ok"] else []
    )
    if args.username:
        candidates = [args.username]
    else:
        candidates = country_players[: args.candidates]

    if not candidates:
        print("\nNo usernames available -- cannot probe player endpoints.")
    else:
        print(f"\nPlayer-level probes using candidates: {', '.join(candidates)}\n")

    profile_probe: dict[str, Any] | None = None
    games_analysis: dict[str, Any] | None = None
    used_player: str | None = None

    for username in candidates:
        print(f"  -- {username}")
        profile_probe = fetch(
            session, f"profile ({username})", f"{BASE}/player/{username}", args.delay
        )
        probes.append(profile_probe)

        probes.append(
            fetch(
                session,
                f"stats ({username})",
                f"{BASE}/player/{username}/stats",
                args.delay,
            )
        )

        archives_probe = fetch(
            session,
            f"archives ({username})",
            f"{BASE}/player/{username}/games/archives",
            args.delay,
        )
        probes.append(archives_probe)

        archive_urls: list[str] = (
            archives_probe.get("payload", {}).get("archives", [])
            if archives_probe["ok"]
            else []
        )
        if not archive_urls:
            print(f"         {username} has no archives, trying next candidate")
            continue

        # Most recent month is the densest and most representative.
        games_probe = fetch(
            session, f"games ({username}, latest)", archive_urls[-1], args.delay
        )
        probes.append(games_probe)

        if games_probe["ok"] and "payload" in games_probe:
            games_analysis = analyse_games(games_probe["payload"].get("games", []))
            used_player = username
            break

    # 4. Conditional request. The collector depends on 304s to stay cheap, so
    #    prove the server actually honours If-None-Match before we build on it.
    if profile_probe and profile_probe.get("etag"):
        probes.append(
            fetch(
                session,
                "conditional re-fetch",
                profile_probe["url"],
                args.delay,
                etag=profile_probe["etag"],
            )
        )
    else:
        print("\n  ! No ETag returned, skipping the conditional-request probe.")

    # Drop full payloads unless asked for them -- otherwise the report is huge.
    if not args.full:
        for probe in probes:
            probe.pop("payload", None)

    passed = sum(1 for p in probes if p["ok"])
    report = {
        "generated_at": started_at.isoformat(),
        "config": {
            "country": args.country,
            "username": args.username,
            "delay_seconds": args.delay,
            "contact": args.contact,
            "full_payloads": args.full,
        },
        "summary": {
            "total": len(probes),
            "passed": passed,
            "failed": len(probes) - passed,
            "country_players_returned": len(country_players),
            "player_used_for_games": used_player,
        },
        "clock_and_accuracy_check": games_analysis,
        "probes": probes,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{passed}/{len(probes)} probes ok")
    if games_analysis:
        print(
            f"[%clk] present in {games_analysis['with_clk']}/"
            f"{games_analysis['total_games']} games "
            f"({games_analysis['with_clk_pct']}%) -- "
            f"accuracies in {games_analysis['with_accuracies_pct']}%"
        )
        for time_class, bucket in sorted(games_analysis["by_time_class"].items()):
            print(
                f"  {time_class:<10} {bucket['games']:>4} games, "
                f"{bucket['with_clk']:>4} with clocks"
            )
    print(f"\nReport written to {out_path}")

    return 0 if passed == len(probes) else 1


if __name__ == "__main__":
    sys.exit(main())
