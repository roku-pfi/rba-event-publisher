# AGENTS.md — rba-event-publisher

Decision journal relay for the RBA thesis (ADR-0044): Valkey stream → decisions
read model in Postgres + RabbitMQ. Status: `../docs/plans/status.md`.

## Guardrails

- Do not put business/policy logic here — only record and publish journal entries.
- Consumers are idempotent on `event_id`; an entry is acked only once recorded and published.
- Shared broker/DB URLs come from `../rba-infra`, not a local compose.
