"""RabbitMQ publish helper (topic exchange)."""

from __future__ import annotations

import json
from typing import Any

import pika


class BusPublisher:
    def __init__(self, amqp_url: str, exchange: str) -> None:
        self._params = pika.URLParameters(amqp_url)
        self._exchange = exchange
        self._conn: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def connect(self) -> None:
        self._conn = pika.BlockingConnection(self._params)
        self._channel = self._conn.channel()
        self._channel.exchange_declare(
            exchange=self._exchange, exchange_type="topic", durable=True
        )

    def close(self) -> None:
        if self._conn and self._conn.is_open:
            self._conn.close()

    def publish(self, routing_key: str, payload: dict[str, Any]) -> None:
        assert self._channel is not None
        body = json.dumps(payload, default=str).encode("utf-8")
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
