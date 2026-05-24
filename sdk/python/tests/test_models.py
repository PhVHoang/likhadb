"""Unit tests for request/response models — no server required."""
import pytest
from pydantic import ValidationError

from likhadb.models import (
    CollectionInfo,
    CreateCollectionRequest,
    FlatIndex,
    HnswIndex,
    IvfIndex,
    IvfSq8Index,
    QueryRequest,
    ScoredResult,
    VectorRecord,
)


# ---------------------------------------------------------------------------
# Index configs
# ---------------------------------------------------------------------------


def test_flat_index_default_type():
    idx = FlatIndex()
    assert idx.type == "flat"
    assert idx.model_dump() == {"type": "flat"}


def test_hnsw_index_round_trips():
    idx = HnswIndex(m=16, ef_construction=200, ef_search=50)
    d = idx.model_dump()
    assert d == {"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50}


def test_ivf_index_round_trips():
    idx = IvfIndex(nlist=1024, nprobe=16)
    assert idx.model_dump() == {"type": "ivf", "nlist": 1024, "nprobe": 16}


def test_ivf_sq8_index_round_trips():
    idx = IvfSq8Index(nlist=256, nprobe=8)
    assert idx.model_dump() == {"type": "ivf_sq8", "nlist": 256, "nprobe": 8}


# ---------------------------------------------------------------------------
# CreateCollectionRequest
# ---------------------------------------------------------------------------


def test_create_collection_defaults_to_flat():
    req = CreateCollectionRequest(name="docs", dim=384, metric="cosine")
    d = req.model_dump()
    assert d["index"] == {"type": "flat"}
    assert d["enable_fts"] is False


def test_create_collection_with_hnsw():
    req = CreateCollectionRequest(
        name="docs",
        dim=384,
        metric="l2",
        index=HnswIndex(m=16, ef_construction=200, ef_search=50),
    )
    d = req.model_dump()
    assert d["index"]["type"] == "hnsw"
    assert d["index"]["m"] == 16


def test_create_collection_invalid_metric():
    with pytest.raises(ValidationError):
        CreateCollectionRequest(name="docs", dim=384, metric="euclidean")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# QueryRequest
# ---------------------------------------------------------------------------


def test_query_request_exclude_none():
    req = QueryRequest(vector=[0.1, 0.2], k=5)
    d = req.model_dump(exclude_none=True)
    assert "filter" not in d
    assert d["include_payload"] is False


def test_query_request_with_filter():
    f = {"field": "category", "op": "eq", "value": "news"}
    req = QueryRequest(vector=[0.1], k=3, filter=f, include_payload=True)
    d = req.model_dump(exclude_none=True)
    assert d["filter"] == f
    assert d["include_payload"] is True


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


def test_scored_result_parses():
    result = ScoredResult.model_validate({"id": 42, "score": 0.95})
    assert result.id == 42
    assert result.score == pytest.approx(0.95)
    assert result.payload is None


def test_scored_result_with_payload():
    result = ScoredResult.model_validate(
        {"id": 1, "score": 0.8, "payload": {"title": "hello"}}
    )
    assert result.payload == {"title": "hello"}


def test_vector_record_parses():
    rec = VectorRecord.model_validate({"id": 7, "vector": [0.1, 0.2, 0.3]})
    assert rec.id == 7
    assert rec.vector == pytest.approx([0.1, 0.2, 0.3])


def test_collection_info_parses():
    info = CollectionInfo.model_validate(
        {"name": "docs", "dim": 384, "metric": "cosine", "count": 1000, "index_type": "hnsw"}
    )
    assert info.name == "docs"
    assert info.count == 1000
