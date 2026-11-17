# Authentication and observability

Authentication protects data and metrics while health remains public; metrics expose service activity to operators.

## Sub-features

- `auth-health-public` permits unauthenticated health checks.
- `auth-protected` rejects missing or invalid bearer tokens on protected routes.
- `auth-valid` accepts the run token.
- `metrics-protected` exports Prometheus metrics only with valid auth.

## How to get to it (user POV)

- Send `GET /health`, `GET /collections`, or `GET /metrics` with or without `Authorization: Bearer …`.
- Configure server auth with `LIKHADB_API_TOKEN`.

## Driving it with verify-likhadb

Preconditions:

- The isolated launch generated and configured a nonempty bearer token.

- **Public health.** Curl `/health` without authorization; require `200` and `{"status":"ok"}`.
- **Missing token.** Curl `/collections` without authorization; require `401`.
- **Wrong token.** Curl `/collections` with `Bearer wrong`; require `401`.
- **Valid token.** Run `verify-likhadb request GET /collections`; require `200`.
- **Metrics.** Run `verify-likhadb request GET /metrics`; require `200` and Prometheus text after at least one operation.

## Gotchas

- `/health` being healthy does not prove authenticated readiness.
- Metrics are intentionally protected because labels can reveal collection names.
- The helper's normal `request` command always supplies valid auth; use raw curl for negative auth cases.
