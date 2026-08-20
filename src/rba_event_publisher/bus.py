"""RabbitMQ publish helper (topic exchange).

The publisher is a long-lived process holding one blocking AMQP connection, so
it must survive the broker going away — a restart, a dropped socket, or a missed
heartbeat. Every failure mode looks the same from here: the next ``publish``
raises. So the channel is re-established lazily on use rather than only once at
startup, and the loop keeps the connection warm while it is idle.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pika

logger = logging.getLogger(__name__)

# Everything pika raises for "the link is gone" — connection- and channel-level
# alike — descends from AMQPError. OSError covers the socket dying underneath it.
_LOST = (pika.exceptions.AMQPError, OSError)


class BusPublisher:
    def __init__(self, amqp_url: str, exchange: str) -> None:
        self._params = pika.URLParameters(amqp_url)
        self._exchange = exchange
        self._conn: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    # -- connection lifecycle -------------------------------------------------

    def connect(self) -> None:
        self._conn = pika.BlockingConnection(self._params)
        self._channel = self._conn.channel()
        self._channel.exchange_declare(
            exchange=self._exchange, exchange_type="topic", durable=True
        )

    def is_healthy(self) -> bool:
        return (
            self._conn is not None
            and self._conn.is_open
            and self._channel is not None
            and self._channel.is_open
        )

    def _discard(self) -> None:
        """Drop the current link without letting teardown errors escape."""
        conn, self._conn, self._channel = self._conn, None, None
        if conn is None:
            return
        try:
            if conn.is_open:
                conn.close()
        except Exception:  # noqa: BLE001 — already replacing it
            logger.debug("error closing a dead AMQP connection", exc_info=True)

    def _reconnect(self) -> None:
        self._discard()
        self.connect()

    def keepalive(self) -> None:
        """Service heartbeats while the outbox is empty.

        A blocking connection only talks to the broker inside a blocking call.
        An idle publisher makes none, so without this the broker eventually
        times out the heartbeat and closes the link that ``publish`` depends on.
        Best effort: a failure here is repaired by the next ``publish``.
        """
        if not self.is_healthy():
            return
        try:
            assert self._conn is not None
            self._conn.process_data_events(0)
        except _LOST:
            logger.info("AMQP link lost while idle; will reconnect on next publish")
            self._discard()

    def close(self) -> None:
        self._discard()

    # -- publishing -----------------------------------------------------------

    def _emit(self, routing_key: str, body: bytes) -> None:
        assert self._channel is not None
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                type=routing_key,
            ),
        )

    def publish(self, routing_key: str, payload: dict[str, Any]) -> None:
        """Publish one event, reconnecting once if the link has died.

        Raises if the retry also fails — the caller records the error on the
        outbox row and leaves ``published_at`` NULL, so the row is retried on
        the next poll.
        """
        body = json.dumps(payload, default=str).encode("utf-8")

        if not self.is_healthy():
            self._reconnect()

        try:
            self._emit(routing_key, body)
        except _LOST:
            logger.warning(
                "AMQP publish failed on routing_key=%s; reconnecting and retrying once",
                routing_key,
                exc_info=True,
            )
            self._reconnect()
            self._emit(routing_key, body)
