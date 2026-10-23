"""Relay the decision journal: Valkey stream → decisions read model + RabbitMQ (ADR-0044)."""

from __future__ import annotations

import logging
import socket
import time

import redis
from sqlalchemy import create_engine

from rba_event_publisher.bus import BusPublisher
from rba_event_publisher.config import Settings, get_settings
from rba_event_publisher.readmodel import ReadModelWriter
from rba_event_publisher.stream import Entry, StreamReader

logger = logging.getLogger(__name__)


def relay(entries: list[Entry], writer: ReadModelWriter, bus: BusPublisher) -> list[str]:
    """Record and publish one batch. Returns the ids to acknowledge.

    The read model is written first, in one transaction: if that fails nothing
    is acknowledged and the batch comes back. An entry whose publish fails is
    not acknowledged either; it is reclaimed and retried, and consumers are
    idempotent on event_id.
    """
    writer.write([(e.channel, e.payload) for e in entries])
    done = []
    for entry in entries:
        try:
            bus.publish(entry.channel, entry.payload)
            done.append(entry.id)
        except Exception:  # noqa: BLE001 — leave it pending and continue
            logger.exception("publish failed event_id=%s", entry.payload.get("event_id"))
    return done


def drain_once(
    reader: StreamReader, writer: ReadModelWriter, bus: BusPublisher, batch_size: int, block_ms: int
) -> tuple[int, int]:
    """One batch. Returns ``(attempted, published)``.

    These two numbers must stay separate. Reporting only the batch size hides a
    broker outage completely: every entry fails, the same entries are retried
    forever, and the log still reads as if work is getting done.
    """
    entries = reader.read(batch_size, block_ms)
    if not entries:
        return (0, 0)
    done = relay(entries, writer, bus)
    reader.ack(done)
    return (len(entries), len(done))


def pause_after(attempted: int, published: int, poll_seconds: float) -> float:
    """How long to wait before the next batch.

    The read blocks until entries arrive, so a healthy relay never sleeps.
    Sleeping after every batch once capped it below what the PDP writes under
    load, and profiles lagged minutes behind (e2e, 2026-10-22). Sleep only while
    publishing is failing, so a dead broker is not retried in a tight loop.
    """
    if attempted and not published:
        return poll_seconds
    return 0.0


def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    writer = ReadModelWriter(create_engine(settings.database_url, future=True))
    reader = StreamReader(
        redis.Redis.from_url(settings.redis_url, decode_responses=True),
        settings.stream,
        settings.group,
        settings.consumer or socket.gethostname(),
        claim_idle_ms=settings.claim_idle_ms,
    )
    reader.ensure_group()
    bus = BusPublisher(settings.rabbitmq_url, settings.exchange_name)
    bus.connect()
    logger.info("event-publisher started stream=%s batch=%d", settings.stream, settings.batch_size)
    stalled = 0
    try:
        while True:
            try:
                attempted, published = drain_once(
                    reader, writer, bus, settings.batch_size, settings.block_ms
                )
            except Exception:  # Valkey or Postgres unreachable: nothing was acked
                logger.exception("relay failed; the batch stays pending")
                time.sleep(settings.poll_interval_seconds)
                continue
            if attempted:
                logger.info("published %d/%d event(s)", published, attempted)
            if attempted and not published:
                stalled += 1
                if stalled == 1 or stalled % 30 == 0:
                    logger.error(
                        "publishing is stalled: %d attempt(s) in a row moved nothing "
                        "— the broker link is down and profiles are no longer updating",
                        stalled,
                    )
            else:
                if stalled:
                    logger.info("publishing recovered after %d stalled poll(s)", stalled)
                stalled = 0
                if not attempted:
                    # Idle: no publish means no AMQP traffic, so heartbeats need
                    # servicing or the broker closes the link out from under us.
                    bus.keepalive()
            if settings.once:
                break
            pause = pause_after(attempted, published, settings.poll_interval_seconds)
            if pause:
                time.sleep(pause)
    finally:
        bus.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
