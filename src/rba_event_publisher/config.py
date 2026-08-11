"""Publisher settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Read/write the decision-service outbox (same DB the PDP owns).
    database_url: str = "postgresql+psycopg://rba:rba@localhost:5432/rba_decision"
    rabbitmq_url: str = "amqp://rba:rba@localhost:5672/"
    exchange_name: str = "rba.events"
    poll_interval_seconds: float = 1.0
    batch_size: int = 50
    once: bool = False  # if true, drain one batch and exit (tests / cron)


@lru_cache
def get_settings() -> Settings:
    return Settings()
