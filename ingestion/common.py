from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests

# Chess.com API base, fallback contact and (connect, read) timeout
BASE = "https://api.chess.com/pub"
DEFAULT_CONTACT = "group12@example.invalid"
TIMEOUT = (10, 30)

# Kafka topics shared by the collectors and the Spark jobs
TOPIC_PLAYERS = "players.raw"
TOPIC_GAMES = "games.raw"


# Serial rate-limited GET, as chess.com throttles parallel requests but not serial ones
class Fetcher:
    def __init__(self, session: requests.Session, delay: float) -> None:
        self.session = session
        self.delay = delay
        self.requests_made = 0
        self.raw_bytes = 0

    def get(self, url: str) -> dict[str, Any] | None:
        time.sleep(self.delay)
        response = self.session.get(url, timeout=TIMEOUT)

        # Back off once and retry when rate limited
        if response.status_code == 429:
            wait = float(response.headers.get("Retry-After", 5))
            print(f"    ! 429, backing off {wait}s")
            time.sleep(wait)
            response = self.session.get(url, timeout=TIMEOUT)

        self.requests_made += 1
        self.raw_bytes += len(response.content)

        # Non-200 and non-JSON responses are treated as missing
        if response.status_code != 200:
            return None
        try:
            return response.json()
        except ValueError:
            return None


# HTTP session identifying the project, as chess.com asks for a contact address
def make_fetcher(delay: float = 1.0) -> Fetcher:
    contact = os.environ.get("CHESS_API_CONTACT", DEFAULT_CONTACT)
    if contact == DEFAULT_CONTACT:
        print("WARNING: no contact address set. Set CHESS_API_CONTACT.")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                f"CS4010-USN-Group12 (coursework; contact: {contact})"
            ),
            "Accept": "application/json",
        }
    )
    return Fetcher(session, delay)


# Wraps a raw payload in ingestion metadata so every Kafka message is self-describing
def envelope(run_id: str, source_url: str, payload: Any) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


# Kafka producer keyed by username so per-player ordering is preserved
def make_producer(bootstrap: str) -> Any:
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=50,
        compression_type="gzip",
        max_request_size=8 * 1024 * 1024,
    )


# MongoDB database handle built from MONGO_URI
def mongo_db(name: str = "chess") -> Any:
    from pymongo import MongoClient

    uri = os.environ.get("MONGO_URI") or os.environ.get("MONGO_SERVER")
    if not uri:
        raise SystemExit("MONGO_URI is not set. Put it in .env.")
    return MongoClient(uri, serverSelectionTimeoutMS=10000)[name]