# rba-event-publisher

Drains the **decision-service transactional outbox** and publishes to RabbitMQ.
Keeps the PDP request path independent of broker uptime
([ADR-0011](../docs/decisions/0011-async-outbox-rabbitmq.md)).

Package version: **0.1.0**. CLI: `rba-event-publisher`. This process owns **no
schema DDL** — it reads/writes the `outbox` table that decision-service
created in `rba_decision`.

> Status: [`../docs/plans/status.md`](../docs/plans/status.md). AI: [`AGENTS.md`](AGENTS.md).

## What it does

```
Postgres rba_decision.outbox  (published_at IS NULL)
        → topic exchange rba.events
        → routing key = row.channel  (rba.decision.made.v1)
        → mark published_at (or last_error on failure)
```

- Polls in `id` order, `FOR UPDATE SKIP LOCKED`, batches of `BATCH_SIZE`.
- Payload is the JSON `DecisionMadeEvent` already stored by the PDP.
- No policy / feature / scoring logic here — publish only.
- Publish failures are recorded on the row; the loop continues. There is
  **no DLQ yet** (Phase 4 remainder).

`tests/` exists but has no cases yet (thin slice).

## Layout

```
src/rba_event_publisher/
├── main.py      # poll loop (ONCE=true → one batch then exit)
├── config.py
├── outbox.py    # ORM mirror of decision-service outbox (no create_all)
└── bus.py       # pika topic exchange, persistent messages
```

## Setup

```bash
cd ../rba-infra && docker compose up -d && cd -

python3 -m venv .venv && source .venv/bin/activate
pip install -e ../rba-contracts -e ".[dev]"
rba-event-publisher
# or: ONCE=true rba-event-publisher
```

Decision-service must have created the `outbox` table (start the PDP once).
Consumers (`rba-profile-service`, `rba-audit-service`) bind their own queues
to the same exchange.

## Env

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgresql+psycopg://rba:rba@localhost:5432/rba_decision` |
| `RABBITMQ_URL` | `amqp://rba:rba@localhost:5672/` |
| `EXCHANGE_NAME` | `rba.events` |
| `POLL_INTERVAL_SECONDS` | `1.0` |
| `BATCH_SIZE` | `50` |
| `ONCE` | `false` |

## Guardrails

- Do not put business/policy logic here.
- Consumers are idempotent on `event_id`; this service only marks `published_at`.
- Shared broker/DB URLs come from `../rba-infra`, not a local compose.

## Status

Phase 4 thin slice. Remaining: DLQ, metrics, k8s Deployment. Roadmap:
`../docs/plans/status.md`.
