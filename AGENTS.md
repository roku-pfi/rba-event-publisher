# AGENTS.md — rba-event-publisher

Outbox → RabbitMQ bridge for the RBA thesis. Status: `../docs/plans/status.md`.

## Guardrails

- Do not put business/policy logic here — only publish outbox payloads.
- Consumers are idempotent on `event_id`; this service marks `published_at`.
- Shared broker/DB URLs come from `../rba-infra`, not a local compose.
