# likhadb Python SDK

Python client for [LikhaDB](https://github.com/PhVHoang/likhadb) — the hybrid vector database for the data lakehouse.

## Installation

```sh
pip install likhadb
```

## Quick start

```python
from likhadb import LikhaDB

with LikhaDB("http://localhost:8080", api_token="your-token") as db:
    db.create_collection("docs", dim=384, metric="cosine")

    col = db.collection("docs")
    col.insert(1, vector=[0.1] * 384, payload={"title": "hello world"})

    results = col.search([0.1] * 384, k=5, include_payload=True)
    for r in results:
        print(r.id, r.score, r.payload)
```

## Async usage

```python
import asyncio
from likhadb import AsyncLikhaDB

async def main():
    async with AsyncLikhaDB("http://localhost:8080") as db:
        await db.create_collection("docs", dim=384, metric="cosine")
        col = db.collection("docs")
        await col.insert(1, vector=[0.1] * 384)
        results = await col.search([0.1] * 384, k=5)

asyncio.run(main())
```

`api_token` is optional when the server does not set `LIKHADB_API_TOKEN`.

## Iceberg source binding

Collections can follow an externally written Iceberg table:

```python
db.create_collection(
    "docs",
    dim=384,
    source_binding={
        "source_namespace": ["lake", "embeddings"],
        "source_table": "documents",
        "id_column": "id",
        "vector_column": "embedding",
        "payload_columns": ["title", "body"],
    },
)
```

## Index types

| dict | Index |
|---|---|
| `None` (default) | Flat exact search |
| `{"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50}` | HNSW graph |
| `{"type": "ivf", "nlist": 1024, "nprobe": 16}` | IVF k-means |
| `{"type": "ivf_sq8", "nlist": 1024, "nprobe": 16}` | IVF + SQ8 quantization |

## Hybrid search

```python
results = col.hybrid_search(
    vector=[0.1] * 384,
    text="Rust ownership model",
    k=10,
    rrf_k=60,           # Reciprocal Rank Fusion constant
    include_payload=True,
)
```
