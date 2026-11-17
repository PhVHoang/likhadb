# LikhaDB verification map

This directory is the maintained source for verifying LikhaDB's user-facing REST behavior. Read this index, then the matching feature recipe.

## Baseline preconditions

- Launch an isolated server with a unique run ID, HTTP port, gRPC port, empty data directory, and generated bearer token.
- Run `verify-likhadb doctor`; require the recorded PID, ports, public health, and authenticated collection listing to agree.
- Never drive an instance not started by the current verification run.

## Driving conventions

- Start each recipe from an empty collection list unless stated otherwise.
- Use `verify-likhadb request` so URL and authorization come from recorded run state.
- Assert HTTP status and parsed JSON fields; do not infer success from curl exit code alone.
- Mutations require a second user-facing read and a persistence observation.
- Cleanup scratch data but retain proof artifacts.

## Proof and skip reporting

- Capture the action and resulting state in request/response transcripts.
- Record exact feature and entry point in `assertions.txt`.
- Report a skipped REST, gRPC, or SDK entry point as unverified, not covered by another surface.
- A healthy process alone is not feature proof.

## Features

- [Collections](./collections.md): create, list, inspect, and delete collections.
- [Vector lifecycle](./vector-lifecycle.md): insert, read, delete, and persistent WAL effects.
- [Vector search](./vector-search.md): nearest-neighbor query, payload return, filtering, and query limits.
- [Hybrid search](./hybrid-search.md): FTS-enabled collection and fused vector/text results.
- [Authentication and observability](./auth-observability.md): public health, bearer protection, and metrics.
