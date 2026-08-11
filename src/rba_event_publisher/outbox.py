"""Minimal ORM mirror of decision-service outbox (publisher owns no schema DDL)."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.types import JSON, Uuid


class Base(DeclarativeBase):
    pass


class OutboxRow(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    channel: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


def fetch_unpublished(session: Session, limit: int) -> list[OutboxRow]:
    stmt = (
        select(OutboxRow)
        .where(OutboxRow.published_at.is_(None))
        .order_by(OutboxRow.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(session.scalars(stmt))
