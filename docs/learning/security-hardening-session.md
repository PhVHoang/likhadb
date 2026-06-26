# Understanding the Security Hardening Session (PR #95)

A running checklist of what you should deeply understand. We work through it
together; nothing gets checked off until you've demonstrated mastery.

## Stage 1 — The Problem (the "why it was broken")
- [ ] What "every endpoint is open" actually means for REST and gRPC
- [ ] Why each of the 4 named threats is exploitable (schema discovery, scan exfil, insert-flood DoS, anon Grafana)
- [ ] The 2 extra holes found (arbitrary parquet path I/O, MinIO default creds) and why they're worse/equal
- [ ] *Why* these holes existed in the first place (the implicit assumptions)
- [ ] How the threats relate — which are confidentiality vs availability vs integrity

## Stage 2 — The Solution (the "why this way")
- [ ] Bearer-token auth: constant-time compare, env-gated/opt-in, one shared check for REST+gRPC
- [ ] Router split: public `/health` vs protected everything-else; why `/metrics` is gated
- [ ] `route_layer` vs `layer` in axum (where the middleware actually applies)
- [ ] gRPC `InterceptedService` + `max_decoding_message_size`
- [ ] `k` clamp (`MAX_K`) and why a shared `validate_k`
- [ ] Body limit + **global** vs per-connection concurrency limit (the key design call)
- [ ] Parquet path confinement: canonicalize, `must_exist` import vs export, `starts_with`, disabled-by-default
- [ ] Compose hardening: `:?` env guards, localhost binding
- [ ] Design stance: simplicity, "leave room for mTLS", why bearer not API-keys/mTLS now

## Stage 3 — Edge Cases & Mechanics (the "what breaks if...")
- [ ] `constant_time_eq` — what it protects against, what it still leaks
- [ ] Absolute-path `Path::join` behavior and why it still gets caught
- [ ] `canonicalize` on a not-yet-existing export file
- [ ] The RwLock-across-await rule and how serialized writes shape the DoS story
- [ ] Why auth is opt-in and what the startup warning is for

## Stage 4 — Broader Context (the "why it matters")
- [ ] Defense in depth: transport (mTLS/TLS) vs application (token) layers
- [ ] Breaking-change impact on existing clients & the parquet-disabled-by-default tradeoff
- [ ] Why 9 small verified commits instead of one big one
- [ ] What this unlocks next (mTLS, per-IP rate limiting, key rotation)

---
### Progress log
- Q (user): "Where does a user get LIKHADB_API_TOKEN?" → Covered: token is
  operator-invented (e.g. `openssl rand -hex 32`), set as a server env var, and
  distributed to clients out-of-band. No issuance endpoint — a deliberate
  property of the *static shared token* choice. Tradeoffs surfaced: no
  self-service, one secret for all, rotation = redeploy, no per-client audit.
  (Touches Stage 2 "bearer not API-keys" + Stage 3 "opt-in / env-gated".)
