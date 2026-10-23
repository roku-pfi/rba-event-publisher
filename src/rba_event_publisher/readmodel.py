"""The decisions read model in ``rba_decision`` (ADR-0018, filled here since ADR-0044).

The console lists decisions from this table. The PDP owns its DDL; this is a
mirror of the columns written, nothing more.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from rba_contracts.events import DECISION_MADE_CHANNEL, LOGIN_CONFIRMED_CHANNEL
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Uuid,
    update,
)
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.engine import Engine

# The PDP's columns are JSONB on Postgres.
_JSON = JSON().with_variant(postgresql.JSONB(), "postgresql")

decisions = Table(
    "decisions",
    MetaData(),
    Column("event_id", Uuid(as_uuid=True), primary_key=True),
    Column("tenant_id", String(64)),
    Column("application_id", String(128)),
    Column("user_id", String(128)),
    Column("risk_score", Float),
    Column("risk_level", String(16)),
    Column("action", String(32)),
    Column("enforced_action", String(32)),
    Column("engine_mode", String(16)),
    Column("simulated", Boolean),
    Column("model_version", String(64)),
    Column("policy_version", String(32)),
    Column("feature_schema_version", String(16)),
    Column("fallback", Boolean),
    Column("reasons", _JSON),
    Column("features", _JSON),
    Column("login", _JSON),
    Column("scored_at", DateTime(timezone=True)),
    Column("step_up_passed_at", DateTime(timezone=True)),
)


def row_from_event(event: dict) -> dict:
    return {
        "event_id": UUID(event["event_id"]),
        "tenant_id": event["tenant_id"],
        "application_id": event["application_id"],
        "user_id": event["user_id"],
        "risk_score": event["risk_score"],
        "risk_level": event["risk_level"],
        "action": event["action"],
        "enforced_action": event["enforced_action"],
        "engine_mode": event["engine_mode"],
        "simulated": event.get("simulated", False),
        "model_version": event["model_version"],
        "policy_version": event["policy_version"],
        "feature_schema_version": event["feature_schema_version"],
        "fallback": event.get("fallback", False),
        "reasons": event.get("reasons") or [],
        "features": event.get("features"),
        "login": event.get("login"),
        "scored_at": datetime.fromisoformat(event["occurred_at"]),
    }


class ReadModelWriter:
    """One transaction per batch; an entry seen twice (a reclaim) is skipped.

    Decisions are inserted first, then each confirmation marks its decision's
    step-up as passed (RF-11): a confirmation always follows its decision in
    the stream, so the row is there by then.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        dialect = postgresql if engine.dialect.name == "postgresql" else sqlite
        self._insert = dialect.insert

    def write(self, entries: list[tuple[str, dict]]) -> None:
        made = [p for channel, p in entries if channel == DECISION_MADE_CHANNEL]
        confirmed = [p for channel, p in entries if channel == LOGIN_CONFIRMED_CHANNEL]
        if not made and not confirmed:
            return
        with self._engine.begin() as conn:
            if made:
                stmt = self._insert(decisions).on_conflict_do_nothing(index_elements=["event_id"])
                conn.execute(stmt, [row_from_event(e) for e in made])
            for event in confirmed:
                conn.execute(
                    update(decisions)
                    .where(decisions.c.event_id == UUID(event["decision_event_id"]))
                    .values(step_up_passed_at=datetime.fromisoformat(event["occurred_at"]))
                )
