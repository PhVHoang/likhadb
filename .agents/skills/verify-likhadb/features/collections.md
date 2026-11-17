# Collections

Collections let a user define a vector space and inspect or remove it through REST.

## Sub-features

- `collection-create` creates a named collection with dimension, metric, and index configuration.
- `collection-list` lists collection names.
- `collection-inspect` returns dimension, metric, count, and index type.
- `collection-delete` removes a collection.

## How to get to it (user POV)

- Send REST requests to `/collections` and `/collections/:name`.
- Use the Python SDK `create_collection`, `list_collections`, `get_collection`, and `drop_collection` methods.

## Driving it with verify-likhadb

Preconditions:

- Doctor passes and `GET /collections` returns an empty array.

- **Create.** Run `verify-likhadb request POST /collections '{"name":"docs","dim":3,"metric":"cosine"}'`; require `201`.
- **List.** Run `verify-likhadb request GET /collections`; require `docs` in `collections`.
- **Inspect.** Run `verify-likhadb request GET /collections/docs`; require name `docs`, dim `3`, metric `cosine`, count `0`, and index type `flat`.
- **Delete.** Run `verify-likhadb request DELETE /collections/docs`; require `204`, then require an empty list.

## Gotchas

- Collection names are path segments; URL-encode unusual names.
- Duplicate creation is an error and does not prove a clean baseline.
- FTS behavior requires the server's `fts` feature and `enable_fts: true`.
