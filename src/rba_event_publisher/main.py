"""Poll unpublished outbox rows and publish to RabbitMQ."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from rba_event_publisher.bus import BusPublisher
from rba_event_publisher.config import Settings, get_settings
from rba_event_publisher.outbox import fetch_unpublished

logger = logging.getLogger(__name__)


def drain_once(session: Session, bus: BusPublisher, batch_size: int) -> int:
    rows = fetch_unpublished(session, batch_size)
    if not rows:
        return 0
    now = datetime.now(timezone.utc)
    for row in rows:
        try:
            bus.publish(row.channel, row.payload)
            row.published_at = now
            row.last_error = None
        except Exception as exc:  # noqa: BLE001 — persist and continue
            logger.exception("publish failed event_id=%s", row.event_id)
            row.last_error = str(exc)[:2000]
    session.commit()
    return len(rows)


def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    engine = create_engine(settings.database_url, future=True)
    factory = sessionmaker(bind=engine, future=True)
    bus = BusPublisher(settings.rabbitmq_url, settings.exchange_name)
    bus.connect()
    logger.info(
        "event-publisher started poll=%.1fs batch=%d",
        settings.poll_interval_seconds,
        settings.batch_size,
    )
    try:
        while True:
            with factory() as session:
                n = drain_once(session, bus, settings.batch_size)
                if n:
                    logger.info("published %d outbox row(s)", n)
            if settings.once:
                break
            time.sleep(settings.poll_interval_seconds)
    finally:
        bus.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
