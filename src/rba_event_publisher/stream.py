"""The decision journal's stream in Valkey (ADR-0044), read as a consumer group.

The PDP appends each event with the decision it records. Entries stay pending
until acknowledged, so a publisher that dies mid-batch leaves them for the next
one to claim; acknowledged entries are deleted, keeping the stream small.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import redis

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Entry:
    id: str
    channel: str
    payload: dict


class StreamReader:
    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        group: str,
        consumer: str,
        *,
        claim_idle_ms: int = 30_000,
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._claim_idle_ms = claim_idle_ms

    def ensure_group(self) -> None:
        try:
            self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def read(self, count: int, block_ms: int) -> list[Entry]:
        """Entries another reader left pending first, then new ones."""
        _, claimed, *_ = self._client.xautoclaim(
            self._stream, self._group, self._consumer, self._claim_idle_ms, count=count
        )
        if claimed:
            return _entries(claimed)
        reply = self._client.xreadgroup(
            self._group, self._consumer, {self._stream: ">"}, count=count, block=block_ms
        )
        return _entries(reply[0][1]) if reply else []

    def ack(self, ids: list[str]) -> None:
        if ids:
            pipe = self._client.pipeline(transaction=False)
            pipe.xack(self._stream, self._group, *ids)
            pipe.xdel(self._stream, *ids)
            pipe.execute()


def _entries(raw: list) -> list[Entry]:
    out = []
    for entry_id, fields in raw:
        if not fields:  # deleted while pending
            continue
        out.append(Entry(entry_id, fields["channel"], json.loads(fields["payload"])))
    return out
