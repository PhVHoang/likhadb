---
name: verify-likhadb
description: Verify LikhaDB through its real REST API by launching an isolated server, driving collection and vector-search workflows with curl, and retaining HTTP, log, and persistence evidence. Use after changing LikhaDB server behavior, storage, authentication, metrics, or the Python SDK's REST contract.
---

# Verify LikhaDB

LikhaDB's primary user surface is its REST API. gRPC and the Python SDK are secondary surfaces. Use the shipped `scripts/verify-likhadb` harness to build and drive a disposable REST/gRPC server; do not reuse an instance you did not launch.

## Launch

From the repository root, choose a unique run ID and unused ports, then launch:

```sh
export LIKHADB_VERIFY_RUN_ID="verify-$(date +%Y%m%d-%H%M%S)"
export LIKHADB_VERIFY_HTTP_PORT=18080
export LIKHADB_VERIFY_GRPC_PORT=15051
.agents/skills/verify-likhadb/scripts/verify-likhadb launch
```

The helper runs `cargo build -p likhadb-server`, creates an empty `/tmp/likhadb-verify-$LIKHADB_VERIFY_RUN_ID/data`, starts `target/debug/likhadb-server` with `HTTP_ADDR=127.0.0.1:$LIKHADB_VERIFY_HTTP_PORT`, `GRPC_ADDR=127.0.0.1:$LIKHADB_VERIFY_GRPC_PORT`, and a run-specific bearer token, then waits for both the `likhadb REST listening` log and a usable authenticated `GET /collections`. It records the exact PID and configuration in the scratch directory. Separate runs are safe when run IDs and ports differ.

Teardown is `.agents/skills/verify-likhadb/scripts/verify-likhadb cleanup`. Never kill by process name.

## Doctor

Run this read-only check whenever the instance looks wrong:

```sh
.agents/skills/verify-likhadb/scripts/verify-likhadb doctor
```

Doctor requires the recorded PID to be alive with the expected binary and data-directory arguments, verifies that the recorded process owns both ports, checks public `/health`, and checks authenticated `/collections`. A failure means the instance is not worth driving.

## Drive

Use `.agents/skills/verify-likhadb/scripts/verify-likhadb request METHOD PATH [JSON]` for literal REST calls. It supplies the isolated instance URL and bearer token and prints the response body followed by `HTTP_STATUS=<code>`.

```sh
.agents/skills/verify-likhadb/scripts/verify-likhadb request POST /collections \
  '{"name":"docs","dim":3,"metric":"cosine"}'
.agents/skills/verify-likhadb/scripts/verify-likhadb request POST /collections/docs/vectors \
  '{"id":1,"vector":[1,0,0],"payload":{"title":"alpha"}}'
.agents/skills/verify-likhadb/scripts/verify-likhadb request POST /collections/docs/query \
  '{"vector":[1,0,0],"k":1,"include_payload":true}'
```

Routes and JSON fields come from `crates/likhadb-server/src/routes.rs` and `types.rs`. Prefer these stable route paths and returned JSON fields over internal Rust state. Read the matching file in `features/` before driving a workflow.

## Evidence

Run the complete basic-search proof with:

```sh
.agents/skills/verify-likhadb/scripts/verify-likhadb prove-basic-search
```

Evidence is retained under `artifacts/verification/likhadb/$LIKHADB_VERIFY_RUN_ID/`: `server.log`, numbered request/response transcripts, `assertions.txt`, and `persistence.txt`. Proof must exercise public REST routes rather than Rust setters or test-only endpoints. Capture each mutating request and its response, then confirm the result through a second user-facing read (`GET` and search). Confirm the WAL side effect alongside visible API state. For auth changes, capture public health plus unauthenticated and authenticated protected requests. Mocks are acceptable only at an already-isolated production boundary.

## Cleanup

```sh
.agents/skills/verify-likhadb/scripts/verify-likhadb cleanup
```

Cleanup sends `TERM` only to the PID recorded for this run, waits for graceful shutdown and its final checkpoint, copies the final server log into evidence, and removes only `/tmp/likhadb-verify-$LIKHADB_VERIFY_RUN_ID`. It never removes `artifacts/verification/likhadb/`. If launch or proof fails, run cleanup before retrying.

## Helpers

`scripts/verify-likhadb` is executable. Its commands are `launch`, `doctor`, `request`, `prove-basic-search`, and `cleanup`. All commands except `launch` load the recorded run configuration; keep `LIKHADB_VERIFY_RUN_ID` set to select the same run. `request` is the low-level curl harness. `prove-basic-search` is the repeatable mapped-feature proof and refuses a non-empty baseline.
