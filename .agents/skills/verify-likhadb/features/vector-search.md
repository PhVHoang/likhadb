# Vector search

Vector search returns the nearest IDs by the collection metric, optionally with payloads and metadata filtering.

## Sub-features

- `search-nearest` ranks stored vectors for a query vector.
- `search-payload` includes payloads when requested.
- `search-filter` restricts candidates with a payload predicate.
- `search-limits` rejects zero or excessive `k`.

## How to get to it (user POV)

- Send `POST /collections/:name/query` over REST.
- Use the Python SDK collection `search` method.
- Call the gRPC query method for the secondary RPC surface.

## Driving it with verify-likhadb

Preconditions:

- Doctor passes; `docs` is 3-dimensional and contains ID 1 `[1,0,0]` titled `alpha` and ID 2 `[0,1,0]` titled `beta`.

- **Nearest.** Run `verify-likhadb request POST /collections/docs/query '{"vector":[0.99,0.01,0],"k":1,"include_payload":true}'`; require `200`, first ID `1`, and title `alpha`.
- **Filter.** Query with `"filter":{"field":"title","op":"eq","value":"beta"}`; require only ID `2`.
- **Lower bound.** Query with `k:0`; require `400` and a message identifying the lower bound.
- **Upper bound.** Query with `k:1025`; require `400` and max `1024` in the message.
- **Proof.** `verify-likhadb prove-basic-search` captures create, inserts, GET confirmation, query, assertions, and WAL state.

## Gotchas

- Lower scores are better for the exposed distance metrics.
- Payloads are omitted unless `include_payload` is true.
- REST proof does not verify the gRPC or Python SDK entry points.
