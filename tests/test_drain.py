"""`drain_once` must report what it published, not what it attempted.

The misleading log is half of why the 2026-08-19 outage was invisible: with the
broker down, every row failed and the loop still printed `published 5 outbox
row(s)` on every poll, because it logged the batch size.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from rba_event_publisher.main import drain_once


class Row:
    def __init__(self) -> None:
        self.event_id = uuid4()
        self.channel = "rba.decision.made.v1"
        self.payload = {"event_id": str(self.event_id)}
        self.published_at: datetime | None = None
        self.last_error: str | None = None


class FakeSession:
    def __init__(self, rows: list[Row]) -> None:
        self.rows = rows
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1


class FakeBus:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[str] = []

    def publish(self, routing_key: str, _payload: dict) -> None:
        if self.fail:
            raise RuntimeError("Channel is closed.")
        self.sent.append(routing_key)


@pytest.fixture(autouse=True)
def _rows_come_from_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "rba_event_publisher.main.fetch_unpublished",
        lambda session, limit: session.rows[:limit],
    )


def test_a_healthy_batch_reports_every_row_published() -> None:
    session = FakeSession([Row(), Row(), Row()])
    attempted, published = drain_once(session, FakeBus(), 50)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 3)
    assert all(r.published_at is not None for r in session.rows)
    assert all(r.last_error is None for r in session.rows)


def test_a_broker_outage_reports_zero_published_not_the_batch_size() -> None:
    """The bug: this used to return 3, so the log read as if work happened."""
    session = FakeSession([Row(), Row(), Row()])
    attempted, published = drain_once(session, FakeBus(fail=True), 50)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 0)


def test_failed_rows_stay_unpublished_so_they_are_retried() -> None:
    session = FakeSession([Row(), Row()])
    drain_once(session, FakeBus(fail=True), 50)  # type: ignore[arg-type]

    assert all(r.published_at is None for r in session.rows)
    assert all("Channel is closed." in (r.last_error or "") for r in session.rows)
    assert session.commits == 1, "the error must be persisted, not lost"


def test_an_empty_outbox_is_not_reported_as_work() -> None:
    session = FakeSession([])
    assert drain_once(session, FakeBus(), 50) == (0, 0)  # type: ignore[arg-type]


def test_one_bad_row_does_not_block_the_rest() -> None:
    class FlakyBus(FakeBus):
        def publish(self, routing_key: str, payload: dict) -> None:
            if payload["event_id"] == str(session.rows[1].event_id):
                raise RuntimeError("nope")
            self.sent.append(routing_key)

    session = FakeSession([Row(), Row(), Row()])
    attempted, published = drain_once(session, FlakyBus(), 50)  # type: ignore[arg-type]

    assert (attempted, published) == (3, 2)
    assert session.rows[1].published_at is None
    assert session.rows[0].published_at is not None
    assert session.rows[2].published_at is not None


def test_published_rows_are_stamped_with_a_tz_aware_time() -> None:
    session = FakeSession([Row()])
    drain_once(session, FakeBus(), 50)  # type: ignore[arg-type]

    stamped = session.rows[0].published_at
    assert stamped is not None and stamped.tzinfo is not None
    assert stamped <= datetime.now(timezone.utc)
