"""Publisher settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # The decision journal's stream (ADR-0044) and the decisions read model.
    redis_url: str = "redis://localhost:6379/0"
    stream: str = "rba:events"
    group: str = "event-publisher"
    consumer: str = ""  # default: the hostname (the pod name)
    claim_idle_ms: int = 30_000  # take over entries another publisher left pending
    database_url: str = "postgresql+psycopg://rba:rba@localhost:5432/rba_decision"
    rabbitmq_url: str = "amqp://rba:rba@localhost:5672/"
    exchange_name: str = "rba.events"
    block_ms: int = 1_000
    poll_interval_seconds: float = 1.0  # back-off while publishing fails
    batch_size: int = 200
    once: bool = False  # if true, drain one batch and exit (tests / cron)


@lru_cache
def get_settings() -> Settings:
    return Settings()
