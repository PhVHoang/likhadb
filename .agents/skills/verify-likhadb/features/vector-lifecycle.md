# Vector lifecycle

Vector lifecycle lets a user insert payload-bearing vectors, retrieve them by ID, and delete them durably.

## Sub-features

- `vector-insert` stores an ID, vector, and optional JSON payload.
- `vector-get` returns stored vector and payload.
- `vector-delete` removes an ID.
- `vector-wal` records mutations in the disposable data directory.

## How to get to it (user POV)

- Send REST requests to `/collections/:name/vectors` and `/collections/:name/vectors/:id`.
- Use the Python SDK collection `insert`, `get`, and `delete` methods.

## Driving it with verify-likhadb

Preconditions:

- Doctor passes and a 3-dimensional `docs` collection exists.

- **Insert.** Run `verify-likhadb request POST /collections/docs/vectors '{"id":1,"vector":[1,0,0],"payload":{"title":"alpha"}}'`; require `204`.
- **Read.** Run `verify-likhadb request GET /collections/docs/vectors/1`; require ID `1`, exact vector, and payload title `alpha`.
- **Persistence evidence.** Inspect the run's `data/wal.log`; require it to exist and have nonzero size after mutation.
- **Delete.** Run `verify-likhadb request DELETE /collections/docs/vectors/1`; require `204`, then require `404` from the same GET.

## Gotchas

- Vector dimension must exactly match the collection dimension.
- A `204` has no JSON response body; status is the assertion.
- Cleanup triggers a checkpoint, so inspect the WAL before cleanup and preserve the observation in evidence.
