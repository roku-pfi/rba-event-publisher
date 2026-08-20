"""Regression tests for the broker link.

The publisher wedged permanently in the k3d rehearsal on 2026-08-19: the AMQP
link was established once at startup, RabbitMQ restarted, and every later
publish raised `ChannelWrongStateError` forever while the log still claimed rows
were being published. Logins kept working, so nothing looked broken — but no
profile was ever updated again. These tests pin both halves of that fix.
"""

from __future__ import annotations

import pika
import pytest

from rba_event_publisher.bus import BusPublisher


class FakeChannel:
    def __init__(self, *, fail_times: int = 0) -> None:
        self.is_open = True
        self.published: list[str] = []
        self.declared = 0
        self._fail_times = fail_times

    def exchange_declare(self, **_: object) -> None:
        self.declared += 1

    def basic_publish(self, *, routing_key: str, **_: object) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            self.is_open = False
            raise pika.exceptions.ChannelWrongStateError("Channel is closed.")
        self.published.append(routing_key)


class FakeConnection:
    def __init__(self, channel: FakeChannel) -> None:
        self.is_open = True
        self._channel = channel
        self.data_events = 0

    def channel(self) -> FakeChannel:
        return self._channel

    def process_data_events(self, _timeout: float) -> None:
        self.data_events += 1

    def close(self) -> None:
        self.is_open = False


def bus_over(channels: list[FakeChannel], monkeypatch: pytest.MonkeyPatch) -> BusPublisher:
    """A BusPublisher whose every connect() hands out the next fake channel."""
    conns: list[FakeConnection] = []

    def fake_blocking_connection(_params: object) -> FakeConnection:
        conn = FakeConnection(channels[len(conns)])
        conns.append(conn)
        return conn

    monkeypatch.setattr(pika, "BlockingConnection", fake_blocking_connection)
    bus = BusPublisher("amqp://guest:guest@localhost:5672/", "rba.events")
    bus._conns = conns  # type: ignore[attr-defined]  # for assertions
    return bus


def test_publish_reconnects_after_the_broker_drops_the_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact k3d failure: publish raises once, and must not raise again."""
    dead, fresh = FakeChannel(fail_times=1), FakeChannel()
    bus = bus_over([dead, fresh], monkeypatch)
    bus.connect()

    bus.publish("rba.decision.made.v1", {"event_id": "abc"})

    assert fresh.published == ["rba.decision.made.v1"]
    assert fresh.declared == 1, "the replacement link must redeclare the exchange"


def test_a_dead_link_is_replaced_before_the_next_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dead, fresh = FakeChannel(), FakeChannel()
    bus = bus_over([dead, fresh], monkeypatch)
    bus.connect()
    dead.is_open = False  # broker went away between polls

    bus.publish("rba.decision.made.v1", {"event_id": "abc"})

    assert dead.published == []
    assert fresh.published == ["rba.decision.made.v1"]


def test_publish_gives_up_after_one_retry_so_the_row_is_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A still-dead broker must surface, not spin — the row stays unpublished."""
    bus = bus_over([FakeChannel(fail_times=1), FakeChannel(fail_times=1)], monkeypatch)
    bus.connect()

    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        bus.publish("rba.decision.made.v1", {"event_id": "abc"})


def test_keepalive_services_heartbeats_while_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without this the broker times out an idle publisher's heartbeat."""
    bus = bus_over([FakeChannel()], monkeypatch)
    bus.connect()

    bus.keepalive()

    assert bus._conns[0].data_events == 1  # type: ignore[attr-defined]


def test_keepalive_on_a_dead_link_is_silent_and_clears_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = bus_over([FakeChannel()], monkeypatch)
    bus.connect()
    bus._conns[0].is_open = False  # type: ignore[attr-defined]

    bus.keepalive()  # must not raise

    assert not bus.is_healthy()
