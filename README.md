# rba-event-publisher

Drains the **decision-service transactional outbox** and publishes to RabbitMQ
(`rba.decision.made.v1`). Keeps the PDP path independent of broker uptime.

## Setup

```bash
# shared stack (Redis/Postgres/RabbitMQ)
cd ../rba-infra && docker compose up -d && cd -

python3 -m venv .venv && source .venv/bin/activate
pip install -e ../rba-contracts -e ".[dev]"
rba-event-publisher
# or: ONCE=true rba-event-publisher   # one batch then exit
```

## Env

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgresql+psycopg://rba:rba@localhost:5432/rba_decision` |
| `RABBITMQ_URL` | `amqp://rba:rba@localhost:5672/` |
| `EXCHANGE_NAME` | `rba.events` |
| `POLL_INTERVAL_SECONDS` | `1.0` |
| `BATCH_SIZE` | `50` |
| `ONCE` | `false` |
