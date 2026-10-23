"""`drain_once` must report what it published, not what it attempted.

The misleading log is half of why the 2026-08-19 outage was invisible: with the
broker down, every row failed and the loop still printed `published 5 outbox
row(s)` on every poll, because it logged the batch size.

Since ADR-0044 a batch comes from the journal's stream and also fills the
decisions read model; an entry is acknowledged only once it is recorded and
published.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select

from rba_event_publisher.main import drain_once, pause_after
from rba_event_publisher.readmodel import ReadModelWriter, decisions
from rba_event_publisher.stream import Entry

DECISION = "rba.decision.made.v1"
CONFIRMED = "rba.login.confirmed.v1"


def _decision() -> dict:
    return {
        "event_id": str(uuid4()),
        "occurred_at": datetime(2026, 10, 23, 15, 0, tzinfo=timezone.utc).isoformat(),
        "tenant_id": "roku-bank",
        "application_id": "roku-bank-web",
        "user_id": "usr_rb_martin",
        "risk_score": 0.11,
        "risk_level": "LOW",
        "action": "ALLOW",
        "enforced_action": "REQUIRE_MFA",
        "engine_mode": "MONITOR",
        "simulated": False,
        "model_version": "freeman-0.4.0",
        "policy_version": "2.1.0",
        "feature_schema_version": "1.0.0",
        "fallback": False,
        "reasons": [{"code": "signal_familiar", "signal": "network"}],
        "features": None,
        "login": {"country": "AR"},
    }


def _entries(*channels: str) -> list[Entry]:
    return [
        Entry(f"{i}-0", ch, _decision() if ch == DECISION else {"event_id": str(uuid4())})
        for i, ch in enumerate(channels)
    ]


class FakeReader:
    def __init__(self, entries: list[Entry]) -> None:
        self.entries = entries
        self.acked: list[str] = []

    def read(self, count: int, block_ms: int) -> list[Entry]:
        return self.entries[:count]

    def ack(self, ids: list[str]) -> None:
        self.acked.extend(ids)


class FakeBus:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[str] = []

    def publish(self, routing_key: str, _payload: dict) -> None:
        if self.fail:
            raise RuntimeError("Channel is closed.")
        self.sent.append(routing_key)


@pytest.fixture
def engine():
    engine = create_engine("sqlite+pysqlite://")
    decisions.metadata.create_all(engine)
    return engine


def _stored(engine) -> list:
    with engine.connect() as conn:
        return list(conn.execute(select(decisions.c.event_id, decisions.c.enforced_action)))


def test_a_healthy_batch_is_recorded_published_and_acked(engine) -> None:
    reader = FakeReader(_entries(DECISION, DECISION, CONFIRMED))
    bus = FakeBus()
    attempted, published = drain_once(reader, ReadModelWriter(engine), bus, 50, 0)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 3)
    assert reader.acked == ["0-0", "1-0", "2-0"]
    assert bus.sent == [DECISION, DECISION, CONFIRMED]
    # Only decisions go to the read model.
    assert [r.enforced_action for r in _stored(engine)] == ["REQUIRE_MFA", "REQUIRE_MFA"]


def test_a_broker_outage_reports_zero_published_and_acks_nothing(engine) -> None:
    """The bug: this used to report the batch size, so the log read as if work happened."""
    reader = FakeReader(_entries(DECISION, DECISION, DECISION))
    attempted, published = drain_once(reader, ReadModelWriter(engine), FakeBus(fail=True), 50, 0)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 0)
    assert reader.acked == [], "unacked entries are reclaimed and retried"


def test_a_reclaimed_entry_is_not_recorded_twice(engine) -> None:
    entries = _entries(DECISION)
    writer = ReadModelWriter(engine)
    drain_once(FakeReader(entries), writer, FakeBus(fail=True), 50, 0)  # type: ignore[arg-type]
    drain_once(FakeReader(entries), writer, FakeBus(), 50, 0)  # type: ignore[arg-type]

    assert len(_stored(engine)) == 1


def test_a_read_model_failure_acks_nothing(engine) -> None:
    class BrokenWriter:
        def write(self, events: list[dict]) -> None:
            raise RuntimeError("database unreachable")

    reader = FakeReader(_entries(DECISION))
    bus = FakeBus()
    with pytest.raises(RuntimeError):
        drain_once(reader, BrokenWriter(), bus, 50, 0)  # type: ignore[arg-type]
    assert (reader.acked, bus.sent) == ([], [])


def test_an_empty_stream_is_not_reported_as_work(engine) -> None:
    assert drain_once(FakeReader([]), ReadModelWriter(engine), FakeBus(), 50, 0) == (0, 0)  # type: ignore[arg-type]


def test_one_bad_entry_does_not_block_the_rest(engine) -> None:
    entries = _entries(DECISION, DECISION, DECISION)
    bad = entries[1].payload["event_id"]

    class FlakyBus(FakeBus):
        def publish(self, routing_key: str, payload: dict) -> None:
            if payload["event_id"] == bad:
                raise RuntimeError("nope")
            self.sent.append(routing_key)

    reader = FakeReader(entries)
    attempted, published = drain_once(reader, ReadModelWriter(engine), FlakyBus(), 50, 0)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 2)
    assert reader.acked == ["0-0", "2-0"]


# ---------------------------------------------------------------- pacing


def test_a_healthy_relay_never_sleeps() -> None:
    assert pause_after(200, 200, 1.0) == 0.0
    assert pause_after(12, 12, 1.0) == 0.0
    assert pause_after(0, 0, 1.0) == 0.0  # the read already blocked


def test_a_failing_batch_waits_instead_of_spinning() -> None:
    assert pause_after(50, 0, 1.0) == 1.0
