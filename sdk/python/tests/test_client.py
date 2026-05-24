"""Client tests using respx to mock the httpx transport — no server required."""
import pytest
import respx
import httpx

from likhadb import (
    LikhaDB,
    AsyncLikhaDB,
    LikhaDBNotFoundError,
    LikhaDBConflictError,
    LikhaDBBadRequestError,
)

BASE = "http://localhost:8080"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COLLECTION_INFO = {
    "name": "docs",
    "dim": 384,
    "metric": "cosine",
    "count": 3,
    "index_type": "flat",
}

SCORED_RESULTS = [
    {"id": 1, "score": 0.95, "payload": {"title": "a"}},
    {"id": 3, "score": 0.80, "payload": {"title": "c"}},
]

# ---------------------------------------------------------------------------
# Sync client — collection DDL
# ---------------------------------------------------------------------------


@respx.mock
def test_health():
    respx.get(f"{BASE}/health").mock(return_value=httpx.Response(200, json={"status": "ok"}))
    with LikhaDB(BASE) as db:
        assert db.health() == {"status": "ok"}


@respx.mock
def test_list_collections():
    respx.get(f"{BASE}/collections").mock(
        return_value=httpx.Response(200, json={"collections": ["docs", "news"]})
    )
    with LikhaDB(BASE) as db:
        assert db.list_collections() == ["docs", "news"]


@respx.mock
def test_create_collection_flat():
    route = respx.post(f"{BASE}/collections").mock(return_value=httpx.Response(201))
    with LikhaDB(BASE) as db:
        db.create_collection("docs", dim=384, metric="cosine")
    body = route.calls[0].request.content
    import json
    payload = json.loads(body)
    assert payload["name"] == "docs"
    assert payload["index"]["type"] == "flat"
    assert payload["enable_fts"] is False


@respx.mock
def test_create_collection_hnsw():
    route = respx.post(f"{BASE}/collections").mock(return_value=httpx.Response(201))
    with LikhaDB(BASE) as db:
        db.create_collection(
            "docs", dim=128, metric="l2",
            index={"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50},
        )
    import json
    payload = json.loads(route.calls[0].request.content)
    assert payload["index"] == {"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50}


@respx.mock
def test_create_collection_conflict_raises():
    respx.post(f"{BASE}/collections").mock(
        return_value=httpx.Response(409, json={"error": "collection 'docs' already exists"})
    )
    with LikhaDB(BASE) as db:
        with pytest.raises(LikhaDBConflictError, match="already exists"):
            db.create_collection("docs", dim=384, metric="cosine")


@respx.mock
def test_get_collection():
    respx.get(f"{BASE}/collections/docs").mock(
        return_value=httpx.Response(200, json=COLLECTION_INFO)
    )
    with LikhaDB(BASE) as db:
        info = db.get_collection("docs")
    assert info.name == "docs"
    assert info.dim == 384
    assert info.count == 3


@respx.mock
def test_get_collection_not_found():
    respx.get(f"{BASE}/collections/missing").mock(
        return_value=httpx.Response(404, json={"error": "collection 'missing' not found"})
    )
    with LikhaDB(BASE) as db:
        with pytest.raises(LikhaDBNotFoundError):
            db.get_collection("missing")


@respx.mock
def test_delete_collection():
    respx.delete(f"{BASE}/collections/docs").mock(return_value=httpx.Response(204))
    with LikhaDB(BASE) as db:
        db.delete_collection("docs")


# ---------------------------------------------------------------------------
# Sync client — Collection handle
# ---------------------------------------------------------------------------


@respx.mock
def test_insert_vector():
    route = respx.post(f"{BASE}/collections/docs/vectors").mock(
        return_value=httpx.Response(204)
    )
    with LikhaDB(BASE) as db:
        db.collection("docs").insert(1, [0.1] * 4, payload={"label": "x"})
    import json
    body = json.loads(route.calls[0].request.content)
    assert body["id"] == 1
    assert body["payload"] == {"label": "x"}


@respx.mock
def test_insert_vector_no_payload_omits_field():
    route = respx.post(f"{BASE}/collections/docs/vectors").mock(
        return_value=httpx.Response(204)
    )
    with LikhaDB(BASE) as db:
        db.collection("docs").insert(2, [0.2] * 4)
    import json
    body = json.loads(route.calls[0].request.content)
    assert "payload" not in body


@respx.mock
def test_get_vector():
    respx.get(f"{BASE}/collections/docs/vectors/1").mock(
        return_value=httpx.Response(200, json={"id": 1, "vector": [0.1, 0.2], "payload": None})
    )
    with LikhaDB(BASE) as db:
        rec = db.collection("docs").get(1)
    assert rec.id == 1


@respx.mock
def test_delete_vector():
    respx.delete(f"{BASE}/collections/docs/vectors/1").mock(return_value=httpx.Response(204))
    with LikhaDB(BASE) as db:
        db.collection("docs").delete(1)


@respx.mock
def test_search():
    respx.post(f"{BASE}/collections/docs/query").mock(
        return_value=httpx.Response(200, json={"results": SCORED_RESULTS})
    )
    with LikhaDB(BASE) as db:
        results = db.collection("docs").search([0.1] * 4, k=2, include_payload=True)
    assert len(results) == 2
    assert results[0].id == 1
    assert results[0].score == pytest.approx(0.95)
    assert results[0].payload == {"title": "a"}


@respx.mock
def test_search_filter_sent():
    route = respx.post(f"{BASE}/collections/docs/query").mock(
        return_value=httpx.Response(200, json={"results": []})
    )
    f = {"field": "category", "op": "eq", "value": "news"}
    with LikhaDB(BASE) as db:
        db.collection("docs").search([0.1], k=5, filter=f)
    import json
    body = json.loads(route.calls[0].request.content)
    assert body["filter"] == f


@respx.mock
def test_hybrid_search():
    respx.post(f"{BASE}/collections/docs/hybrid-query").mock(
        return_value=httpx.Response(200, json={"results": SCORED_RESULTS})
    )
    with LikhaDB(BASE) as db:
        results = db.collection("docs").hybrid_search(
            [0.1] * 4, text="hello world", k=2
        )
    assert len(results) == 2


@respx.mock
def test_import_parquet():
    respx.post(f"{BASE}/collections/docs/import-parquet").mock(
        return_value=httpx.Response(200, json={"imported": 42})
    )
    with LikhaDB(BASE) as db:
        n = db.collection("docs").import_parquet(
            "/data/vecs.parquet", id_col="id", vector_col="embedding"
        )
    assert n == 42


@respx.mock
def test_export_parquet():
    respx.post(f"{BASE}/collections/docs/export-parquet").mock(
        return_value=httpx.Response(204)
    )
    with LikhaDB(BASE) as db:
        db.collection("docs").export_parquet("/data/out.parquet")


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@respx.mock
def test_bad_request_raises():
    respx.post(f"{BASE}/collections/docs/query").mock(
        return_value=httpx.Response(400, json={"error": "dim mismatch"})
    )
    with LikhaDB(BASE) as db:
        with pytest.raises(LikhaDBBadRequestError, match="dim mismatch"):
            db.collection("docs").search([0.1], k=1)


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


@respx.mock
async def test_async_health():
    respx.get(f"{BASE}/health").mock(return_value=httpx.Response(200, json={"status": "ok"}))
    async with AsyncLikhaDB(BASE) as db:
        assert await db.health() == {"status": "ok"}


@respx.mock
async def test_async_list_collections():
    respx.get(f"{BASE}/collections").mock(
        return_value=httpx.Response(200, json={"collections": ["a", "b"]})
    )
    async with AsyncLikhaDB(BASE) as db:
        assert await db.list_collections() == ["a", "b"]


@respx.mock
async def test_async_create_and_search():
    respx.post(f"{BASE}/collections").mock(return_value=httpx.Response(201))
    respx.post(f"{BASE}/collections/docs/query").mock(
        return_value=httpx.Response(200, json={"results": SCORED_RESULTS})
    )
    async with AsyncLikhaDB(BASE) as db:
        await db.create_collection("docs", dim=4, metric="cosine")
        col = db.collection("docs")
        results = await col.search([0.1] * 4, k=2)
    assert results[0].id == 1


@respx.mock
async def test_async_not_found():
    respx.get(f"{BASE}/collections/ghost").mock(
        return_value=httpx.Response(404, json={"error": "not found"})
    )
    async with AsyncLikhaDB(BASE) as db:
        with pytest.raises(LikhaDBNotFoundError):
            await db.get_collection("ghost")


# ---------------------------------------------------------------------------
# _parse_index helper
# ---------------------------------------------------------------------------


def test_parse_index_unknown_raises():
    from likhadb.client import _parse_index
    with pytest.raises(ValueError, match="Unknown index type"):
        _parse_index({"type": "faiss"})


def test_parse_index_none_gives_flat():
    from likhadb.client import _parse_index
    from likhadb.models import FlatIndex
    assert isinstance(_parse_index(None), FlatIndex)
