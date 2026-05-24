from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field

VecId = int
Vector = list[float]
Payload = dict[str, Any]

# ---------------------------------------------------------------------------
# Index configs — mirror IndexConfig in likhadb-server/src/types.rs
# ---------------------------------------------------------------------------


class FlatIndex(BaseModel):
    type: Literal["flat"] = "flat"


class IvfIndex(BaseModel):
    type: Literal["ivf"] = "ivf"
    nlist: int
    nprobe: int


class IvfSq8Index(BaseModel):
    type: Literal["ivf_sq8"] = "ivf_sq8"
    nlist: int
    nprobe: int


class HnswIndex(BaseModel):
    type: Literal["hnsw"] = "hnsw"
    m: int
    ef_construction: int
    ef_search: int


IndexConfig = Annotated[
    Union[FlatIndex, IvfIndex, IvfSq8Index, HnswIndex],
    Field(discriminator="type"),
]

# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class CreateCollectionRequest(BaseModel):
    name: str
    dim: int
    metric: Literal["l2", "cosine", "dot"]
    index: IndexConfig = Field(default_factory=FlatIndex)
    enable_fts: bool = False


class InsertRequest(BaseModel):
    id: VecId
    vector: Vector
    payload: Payload | None = None


class QueryRequest(BaseModel):
    vector: Vector
    k: int
    filter: Any | None = None
    include_payload: bool = False


class HybridQueryRequest(BaseModel):
    vector: Vector
    text: str
    k: int
    rrf_k: int = 60
    filter: Any | None = None
    include_payload: bool = False


class ImportParquetRequest(BaseModel):
    path: str
    id_col: str
    vector_col: str
    payload_cols: list[str] = Field(default_factory=list)


class ExportParquetRequest(BaseModel):
    path: str


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ScoredResult(BaseModel):
    id: VecId
    score: float
    payload: Payload | None = None


class VectorRecord(BaseModel):
    id: VecId
    vector: Vector
    payload: Payload | None = None


class CollectionInfo(BaseModel):
    name: str
    dim: int
    metric: str
    count: int
    index_type: str


class ImportResponse(BaseModel):
    imported: int
