"""The document model: collections, indexes, and the projections that turn raw
Chess.com payloads into stored documents. See docs/data-model.md.
"""

from __future__ import annotations

import io
from typing import Any

PLAYERS = "players"
GAMES = "games"
GAME_MOVES = "game_moves"

LIVE_CLASSES = ("blitz", "rapid", "bullet")


# --- Indexes ---

INDEXES = {
    PLAYERS: [
        [("country", 1), ("sampled", 1)],
        [("blitz.rating", 1)],
        [("updated", -1)],
    ],
    GAMES: [
        [("time_class", 1), ("end_time", -1)],
        [("white", 1)],
        [("black", 1)],
    ],
}


def ensure_indexes(db: Any) -> list[str]:
    """Safe to re-run."""
    created = []
    for collection, keysets in INDEXES.items():
        for keys in keysets:
            name = db[collection].create_index(keys)
            created.append(f"{collection}.{name}")
    return created


# --- Players ---

def build_player(
    profile: dict[str, Any], stats: dict[str, Any], date: str
) -> dict[str, Any]:
    """One document per player. `_id` is the lowercase username, the only id the
    profile and game endpoints share."""
    country = profile.get("country")  # '.../country/NO' -> 'NO'

    doc: dict[str, Any] = {
        "_id": (profile.get("username") or "").lower(),
        "username": profile.get("username"),
        "country": country.rsplit("/", 1)[-1] if country else None,
        "title": profile.get("title"),
        "status": profile.get("status"),
        "sampled": True,
        "updated": date,
        "last_online": profile.get("last_online"),
    }

    # Absent when never played. Never imputed to zero.
    for cls in LIVE_CLASSES:
        block = stats.get(f"chess_{cls}")
        if block:
            doc[cls] = _time_class_block(block)

    # Only the extremes are exposed, there is no current tactics rating.
    tactics = stats.get("tactics") or {}
    if tactics:
        doc["tactics_high"] = (tactics.get("highest") or {}).get("rating")
        doc["tactics_low"] = (tactics.get("lowest") or {}).get("rating")

    rush = (stats.get("puzzle_rush") or {}).get("best") or {}
    if rush:
        doc["puzzle_rush"] = rush.get("score")

    return doc


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


def build_stub_player(ref: dict[str, Any]) -> dict[str, Any] | None:
    """Identity-only document for an opponent outside the sampled cohort, built
    from what the game record already carries."""
    at_id = ref.get("@id")  # games identify players by '@id', not numeric id
    if not at_id:
        return None

    return {
        "_id": at_id.rsplit("/", 1)[-1].lower(),
        "username": ref.get("username"),
        "uuid": ref.get("uuid"),
        "sampled": False,
    }


# --- Games ---

def game_reject_reason(game: dict[str, Any], end_time_floor: int = 0) -> str | None:
    """None if the game should be kept, otherwise the reason code."""
    if game.get("rules") != "chess":
        return f"variant:{game.get('rules')}"

    if game.get("time_class") not in LIVE_CLASSES:
        return f"time_class:{game.get('time_class')}"

    if not game.get("rated"):
        return "unrated"

    if end_time_floor and (game.get("end_time") or 0) < end_time_floor:
        return "too_old"

    return None


def build_game(payload: dict[str, Any], white_id: str, black_id: str) -> dict[str, Any]:
    """Game metadata. The movetext goes to game_moves separately."""
    white = payload.get("white") or {}
    black = payload.get("black") or {}
    base, increment = parse_time_control(payload.get("time_control"))

    if white.get("result") == "win":
        result = "w"
    elif black.get("result") == "win":
        result = "b"
    else:
        result = "d"

    # Only the loser's result says how it ended: resigned, timeout, mated.
    termination = black.get("result") if result == "w" else white.get("result")

    tail = (payload.get("url") or "").rstrip("/").rsplit("/", 1)[-1]  # -> 173838343716

    doc = {
        "_id": int(tail) if tail.isdigit() else None,
        "white": white_id,
        "black": black_id,
        "white_rating": white.get("rating"),
        "black_rating": black.get("rating"),
        "result": result,
        "termination": termination,
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


def parse_time_control(tc: str | None) -> tuple[int | None, int]:
    """'180' -> (180, 0); '180+2' -> (180, 2); '1/259200' (daily) -> (None, 0)."""
    if not tc or "/" in tc:
        return None, 0

    if "+" in tc:
        base, inc = tc.split("+", 1)
        return (int(base) if base.isdigit() else None, int(inc) if inc.isdigit() else 0)

    return (int(tc) if tc.isdigit() else None, 0)


def parse_movetext(
    pgn: str, base: int | None, increment: int
) -> dict[str, list[Any]] | None:
    """SAN moves, clock left per ply, time spent per ply. None if unparseable."""
    import chess.pgn

    game = chess.pgn.read_game(io.StringIO(pgn))
    if game is None:
        return None

    # Replayed rather than scraped, so an illegal move shows up as bad data.
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

    # Plies alternate sides, so two back is the same player's previous clock.
    spent: list[float | None] = []
    for i, clock in enumerate(clocks):
        if clock is None:
            spent.append(None)
            continue

        previous = clocks[i - 2] if i >= 2 else base
        spent.append(
            round(previous - clock + increment, 1) if previous is not None else None
        )

    if not moves:
        return None

    return {"moves": moves, "clocks": clocks, "spent": spent}