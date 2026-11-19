# Hybrid search

Hybrid search fuses vector similarity and BM25 text ranks for an FTS-enabled collection.

## Sub-features

- `hybrid-enable-fts` enables full-text indexing at collection creation.
- `hybrid-query` combines a vector and text query.
- `hybrid-payload` returns matching payloads when requested.

## How to get to it (user POV)

- Create a collection with `enable_fts: true`, insert text payloads, then send `POST /collections/:name/hybrid-query`.
- Use the Python SDK collection `hybrid_search` method.

## Driving it with verify-likhadb

Preconditions:

- Doctor passes. The `likhadb-server` crate enables `likhadb-store/fts` in its normal build.

- **Create.** Create `articles` with dimension 3, cosine metric, and `enable_fts:true`; require `201`.
- **Seed.** Insert distinct Rust and Python payloads; require `204` for each.
- **Query.** Run `verify-likhadb request POST /collections/articles/hybrid-query '{"vector":[1,0,0],"text":"Rust ownership","k":2,"include_payload":true}'`; require `200` and the Rust payload among results.

## Gotchas

- A custom server build that removes `likhadb-store/fts` cannot prove this recipe; report the path unverified rather than treating vector-only search as equivalent.
- Newly inserted string payload values are the indexed text.
- RRF scores are fusion scores, not raw vector distances.
