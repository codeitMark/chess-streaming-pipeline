#!/usr/bin/env python3
"""Shared ingestion plumbing: the rate-limited HTTP session and the sinks.

Both collectors import this. The sink abstraction exists so a collector can be
run against a JSON file (no infrastructure needed) or against Kafka (the real
path) without changing any collection logic.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import requests

BASE = "https://api.chess.com/pub"
DEFAULT_CONTACT = "group12@example.invalid"
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30


def contact_address(explicit: str | None = None) -> str:
    address = explicit or os.environ.get("CHESS_API_CONTACT", DEFAULT_CONTACT)
    if address == DEFAULT_CONTACT:
        print("WARNING: no contact address set. Set CHESS_API_CONTACT.")
    return address


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
    """Serial, rate-limited GET with ETag support and 429 back-off.

    Serial is not a performance choice. Chess.com states that serial access is
    unlimited but that parallel requests may return 429 and that abnormal
    activity can get an application blocked. Do not add threads here.
    """

    def __init__(self, session: requests.Session, delay: float, run_id: str) -> None:
        self.session = session
        self.delay = delay
        self.run_id = run_id
        self.requests_made = 0
        self.not_modified = 0
        self.raw_bytes = 0
        self.etags: dict[str, str] = {}

    def get(self, url: str) -> dict[str, Any] | None:
        time.sleep(self.delay)

        headers = {}
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
        if response.headers.get("ETag"):
            self.etags[url] = response.headers["ETag"]

        if response.status_code == 304:
            self.not_modified += 1
            return None
        if response.status_code != 200:
            return None
        try:
            return response.json()
        except ValueError:
            return None


# --------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------


class Sink(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


def envelope(run_id: str, source_url: str, payload: Any) -> dict[str, Any]:
    """Ingestion metadata wrapped around every raw payload.

    The consumer strips this before storing -- it exists so a message on the
    broker is self-describing during the 14-day replay window.
    """
    return {
        "run_id": run_id,
        "source_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


class KafkaSink:
    """The real path. Keyed by username so per-player ordering is preserved."""

    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            linger_ms=50,
            compression_type="gzip",
            max_request_size=8 * 1024 * 1024,
        )
        self.sent = 0

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.producer.send(topic, key=key, value=value)
        self.sent += 1

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


class FileSink:
    """No-infrastructure fallback: one newline-delimited JSON file per topic.

    Each line is exactly what would have been one Kafka message, so the consumer
    can be pointed at these files for testing without a broker running.
    """

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.handles: dict[str, Any] = {}
        self.sent = 0

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if topic not in self.handles:
            self.handles[topic] = (self.out_dir / f"{topic}.jsonl").open(
                "w", encoding="utf-8"
            )
        self.handles[topic].write(
            json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n"
        )
        self.sent += 1

    def close(self) -> None:
        for handle in self.handles.values():
            handle.close()


def make_sink(target: str, bootstrap: str, out_dir: Path) -> Sink:
    if target == "kafka":
        return KafkaSink(bootstrap)
    return FileSink(out_dir)


def mongo_db(uri: str | None = None, name: str = "chess") -> Any:
    from pymongo import MongoClient

    uri = uri or os.environ.get("MONGO_URI") or os.environ.get("MONGO_SERVER")
    if not uri:
        raise SystemExit("MONGO_URI is not set. Put it in .env.")
    return MongoClient(uri, serverSelectionTimeoutMS=10000)[name]
