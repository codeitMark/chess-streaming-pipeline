"""Stage 1a -- player collection.

Two requests per player, no game archives. Produces one message per player
carrying profile and stats together, which the consumer writes as a single
document. Game collection is a separate process (collect_games.py) because its
cost per player is wildly uneven and would otherwise stall this loop.

    python ingestion/collect_players.py --countries NO,IS,DK --players 200
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.common import (  # noqa: E402
    BASE,
    Fetcher,
    build_session,
    contact_address,
    envelope,
    make_sink,
)
from processing.schema import TOPIC_PLAYERS  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect daily player snapshots.")
    parser.add_argument("--countries", default="NO")
    parser.add_argument(
        "--players", type=int, default=100,
        help="Players per country. The roster endpoint caps at 10000 and ignores "
             "every pagination parameter, so that is the hard ceiling.",
    )
    parser.add_argument(
        "--offset", type=int, default=0,
        help="Skip this many usernames in each roster. The roster is alphabetical "
             "and stable, so offsets let you cover it across several runs.",
    )
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--contact", default=None)
    parser.add_argument("--target", choices=("kafka", "file"), default="file")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        help="Defaults to $KAFKA_BOOTSTRAP_SERVERS. Inside compose that is "
             "kafka:9092; from the host it is localhost:9094.",
    )
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc)
    out_dir = Path(
        args.out or Path(__file__).parent / "out" / f"players_{started:%Y%m%dT%H%M%SZ}"
    )

    fetcher = Fetcher(build_session(contact_address(args.contact)), args.delay, run_id)
    sink = make_sink(args.target, args.bootstrap, out_dir)
    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]

    print(f"run {run_id} -> {args.target} | {countries} | {args.players} each\n")

    emitted = 0
    for iso in countries:
        listing = fetcher.get(f"{BASE}/country/{iso}/players")
        if not listing:
            print(f"[{iso}] roster unavailable")
            continue

        roster = listing.get("players", [])
        window = roster[args.offset : args.offset + args.players]
        print(f"[{iso}] roster {len(roster)}, taking {len(window)} from {args.offset}")

        for i, username in enumerate(window, 1):
            profile = fetcher.get(f"{BASE}/player/{username}")
            if not profile:
                continue
            stats = fetcher.get(f"{BASE}/player/{username}/stats") or {}

            # One message carries both halves so the consumer can build the
            # player document and its dated snapshot in a single transaction.
            sink.send(
                TOPIC_PLAYERS,
                username,
                envelope(
                    run_id,
                    f"{BASE}/player/{username}",
                    {"profile": profile, "stats": stats, "country_hint": iso},
                ),
            )
            emitted += 1
            if i % 25 == 0:
                print(f"  {i}/{len(window)}")

    sink.close()
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(
        f"\n{emitted} players | {fetcher.requests_made} requests "
        f"({fetcher.not_modified} x 304) | {fetcher.raw_bytes:,} B | {elapsed/60:.1f} min"
    )
    if args.target == "file":
        print(f"written to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
