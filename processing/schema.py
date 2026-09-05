#!/usr/bin/env python3
"""The data model as code: collection names, indexes, and the projections that
turn raw Chess.com payloads into stored documents.

Imported by the consumer and the Spark jobs so there is exactly one definition
of what a document looks like. See docs/data-model.md for the reasoning.
"""

from __future__ import annotations

import io
import re
from typing import Any, Iterable

# --------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------

TOPIC_PLAYERS = "players.raw"
TOPIC_GAMES = "games.raw"

PLAYERS = "players"
GAMES = "games"
GAME_MOVES = "game_moves"
POSITIONS = "positions"
MOVE_EVALS = "move_evals"

LIVE_CLASSES = ("blitz", "rapid", "bullet")

# Our own documented thresholds, in centipawns of loss against the engine's best
# move. Deliberately ours rather than saulochess's, so we can explain them --
# saulochess then becomes a second labelling scheme to compare against.
BLUNDER_THRESHOLDS = ((50, "ok"), (100, "inaccuracy"), (300, "mistake"))
BLUNDER_LABEL = "blunder"

INDEXES: dict[str, list[tuple[list[tuple[str, int]], bool]]] = {
    #  collection: [ (keys, unique), ... ]
    PLAYERS: [
        ([("country", 1), ("sampled", 1)], False),
        ([("blitz.rating", 1)], False),   # stratified selection by rating band
        ([("updated", -1)], False),       # "recently collected" selection
    ],
    GAMES: [
        ([("time_class", 1), ("end_time", -1)], False),
        ([("white", 1)], False),
        ([("black", 1)], False),
    ],
    POSITIONS: [([("fen", 1), ("engine", 1)], True)],
    MOVE_EVALS: [([("game", 1), ("ply", 1)], True), ([("player", 1), ("cls", 1)], False)],
}


def ensure_indexes(db: Any) -> list[str]:
    """Create every index the model needs. Safe to re-run."""
    created = []
    for collection, specs in INDEXES.items():
        for keys, unique in specs:
            name = db[collection].create_index(keys, unique=unique)
            created.append(f"{collection}.{name}")
    return created


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def iso_country(url: str | None) -> str | None:
    """'https://api.chess.com/pub/country/NO' -> 'NO'."""
    return url.rsplit("/", 1)[-1] if url else None


def uname_of(ref: dict[str, Any]) -> str | None:
    """Game records identify players by '@id', not by numeric id."""
    at_id = ref.get("@id")
    return at_id.rsplit("/", 1)[-1].lower() if at_id else None


def game_id(url: str | None) -> int | None:
    """'https://www.chess.com/game/live/173838343716' -> 173838343716."""
    if not url:
        return None
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    return int(tail) if tail.isdigit() else None


def parse_time_control(tc: str | None) -> tuple[int | None, int]:
    """'180' -> (180, 0);  '180+2' -> (180, 2);  '1/259200' (daily) -> (None, 0).

    Parsed once here rather than in every Spark row.
    """
    if not tc or "/" in tc:
        return None, 0
    if "+" in tc:
        base, inc = tc.split("+", 1)
        return (int(base) if base.isdigit() else None, int(inc) if inc.isdigit() else 0)
    return (int(tc) if tc.isdigit() else None, 0)


# --------------------------------------------------------------------------
# Projections
# --------------------------------------------------------------------------


def build_player(
    profile: dict[str, Any], stats: dict[str, Any], date: str
) -> dict[str, Any]:
    """One document per player: identity and current stats together.

    Stats are embedded rather than kept in a referencing collection because the
    relationship is 1:1, both halves are small and similar in size, they are
    written by the same message, and essentially every analytical query wants
    both at once -- "mean blitz rating by country" needs `country` and
    `blitz.rating` in the same document. Splitting them would add a $lookup to
    every query and buy nothing.

    That is the opposite call from `games` / `game_moves`, and deliberately so:
    there the movetext is 69% of the document and the dashboard never reads it,
    so the sizes and access patterns diverge. The rule is embed 1:1 data of
    similar size that is read together, split when size or access diverges.

    `_id` is the lowercase username, which is forced on us: the profile endpoint
    returns a numeric `player_id` but no uuid, while game records carry a uuid
    and `@id` but no numeric id. The username is the only identifier the two
    share, so it has to be the key -- even though §3 correctly warns that
    usernames change.

    `joined` and `player_id` are not stored: static profile properties can be
    re-fetched at any time. `status` is kept because §3's fair-play comparison
    needs it.
    """
    doc: dict[str, Any] = {
        "_id": (profile.get("username") or "").lower(),
        "username": profile.get("username"),
        "country": iso_country(profile.get("country")),
        "title": profile.get("title"),
        "status": profile.get("status"),
        "sampled": True,
        "updated": date,
        "last_online": profile.get("last_online"),
    }

    for cls in LIVE_CLASSES:
        block = stats.get(f"chess_{cls}")
        # Absent when never played. Never imputed -- absent is not zero.
        if block:
            doc[cls] = _time_class_block(block)

    tactics = stats.get("tactics") or {}
    if tactics:
        # Only the extremes are exposed; there is no current tactics rating.
        doc["tactics_high"] = (tactics.get("highest") or {}).get("rating")
        doc["tactics_low"] = (tactics.get("lowest") or {}).get("rating")

    rush = (stats.get("puzzle_rush") or {}).get("best") or {}
    if rush:
        doc["puzzle_rush"] = rush.get("score")

    return doc


def build_stub_player(ref: dict[str, Any]) -> dict[str, Any] | None:
    """Identity-only document for an opponent we have never profiled.

    Almost every game has one player inside our sampled cohort and one outside
    it. Requiring both sides to be fully profiled would reject nearly the entire
    corpus, so opponents are created from what the game record already carries.
    `sampled: False` keeps them out of any cohort-based analysis, while their
    ratings still count as observations for the distribution questions.
    """
    uname = uname_of(ref)
    if not uname:
        return None
    return {"_id": uname, "username": ref.get("username"), "uuid": ref.get("uuid"),
            "sampled": False}


def _time_class_block(block: dict[str, Any]) -> dict[str, Any]:
    last = block.get("last") or {}
    record = block.get("record") or {}
    return {
        "rating": last.get("rating"),
        "rd": last.get("rd"),
        "w": record.get("win"),
        "l": record.get("loss"),
        "d": record.get("draw"),
    }


def build_game(payload: dict[str, Any], white_id: str, black_id: str) -> dict[str, Any]:
    """Game metadata. The movetext goes to game_moves separately."""
    base, increment = parse_time_control(payload.get("time_control"))
    white = payload.get("white") or {}
    black = payload.get("black") or {}

    if white.get("result") == "win":
        result = "w"
    elif black.get("result") == "win":
        result = "b"
    else:
        result = "d"

    doc = {
        "_id": game_id(payload.get("url")),
        "white": white_id,
        "black": black_id,
        "white_rating": white.get("rating"),
        "black_rating": black.get("rating"),
        "result": result,
        "termination": (white.get("result") if result != "w" else black.get("result")),
        "time_class": payload.get("time_class"),
        "time_control": payload.get("time_control"),
        "base": base,
        "increment": increment,
        "end_time": payload.get("end_time"),
    }
    if payload.get("accuracies"):
        doc["accuracies"] = {
            "white": payload["accuracies"].get("white"),
            "black": payload["accuracies"].get("black"),
        }
    return doc


CLK_RE = re.compile(r"\[%clk\s+(\d+):(\d+):(\d+(?:\.\d+)?)\]")


def parse_movetext(
    pgn: str, base: int | None, increment: int
) -> dict[str, list[Any]] | None:
    """SAN moves, clock remaining per ply, and time spent per ply.

    Uses python-chess so the moves are validated rather than regex-scraped; a
    game that does not replay legally is a data-quality problem we want to see.
    Returns None if the movetext cannot be parsed at all.
    """
    import chess.pgn

    game = chess.pgn.read_game(io.StringIO(pgn))
    if game is None:
        return None

    board = game.board()
    moves: list[str] = []
    clocks: list[float | None] = []

    node: Any = game
    while node.variations:
        node = node.variation(0)
        try:
            moves.append(board.san(node.move))
            board.push(node.move)
        except (ValueError, AssertionError):
            return None
        clocks.append(node.clock())

    # Time spent on ply i is that side's previous clock minus the current one,
    # plus whatever increment they were given for making the move.
    spent: list[float | None] = []
    for i, clock in enumerate(clocks):
        if clock is None:
            spent.append(None)
            continue
        previous = clocks[i - 2] if i >= 2 else base
        spent.append(
            round(previous - clock + increment, 1) if previous is not None else None
        )

    return {"moves": moves, "clocks": clocks, "spent": spent}


def build_moves(game_doc_id: int, parsed: dict[str, list[Any]]) -> dict[str, Any]:
    return {"_id": game_doc_id, **parsed}


def classify(cpl: int) -> str:
    """Centipawn loss -> our blunder label."""
    for threshold, label in BLUNDER_THRESHOLDS:
        if cpl < threshold:
            return label
    return BLUNDER_LABEL


def game_reject_reason(game: dict[str, Any], end_time_floor: int = 0) -> str | None:
    """None if the game should be kept, otherwise the reason code.

    Reasons are counted and reported locally by the consumer, not stored. The
    filters are deterministic and the raw payload stays on the broker for the
    retention window, so a rejected record is reproducible without a table of
    them in the database.
    """
    if game.get("rules") != "chess":
        return f"variant:{game.get('rules')}"
    if game.get("time_class") not in LIVE_CLASSES:
        return f"time_class:{game.get('time_class')}"
    if not game.get("rated"):
        return "unrated"
    # The three most recent *archives* are not the three most recent months --
    # inactive players reached back as far as 2012 in the first real run.
    if end_time_floor and (game.get("end_time") or 0) < end_time_floor:
        return "too_old"
    return None
