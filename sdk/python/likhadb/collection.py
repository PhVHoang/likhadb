"""Collection handles — sync and async — for vector operations."""

from __future__ import annotations

from typing import Any

from ._http import AsyncHttpClient, HttpClient
from .models import (
    CollectionInfo,
    ExportParquetRequest,
    HybridQueryRequest,
    ImportParquetRequest,
    InsertRequest,
    PipelineResult,
    QueryRequest,
    ScoredResult,
    VectorRecord,
)

SearchResult = ScoredResult | PipelineResult


def _parse_search_results(items: list) -> list[SearchResult]:
    if items and "fusion_score" in items[0]:
        return [PipelineResult.model_validate(item) for item in items]
    return [ScoredResult.model_validate(item) for item in items]


class Collection:
    """Sync handle to a single LikhaDB collection."""

    def __init__(self, name: str, http: HttpClient) -> None:
        self._name = name
        self._http = http

    @property
    def name(self) -> str:
        return self._name

    def info(self) -> CollectionInfo:
        """Return collection metadata (dim, metric, count, index type)."""
        r = self._http.get(f"/collections/{self._name}")
        return CollectionInfo.model_validate(r.json())

    def insert(
        self,
        id: int,
        vector: list[float],
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Insert or overwrite a vector."""
        req = InsertRequest(id=id, vector=vector, payload=payload)
        self._http.post(
            f"/collections/{self._name}/vectors",
            json=req.model_dump(exclude_none=True),
        )

    def get(self, id: int) -> VectorRecord:
        """Retrieve a vector by ID."""
        r = self._http.get(f"/collections/{self._name}/vectors/{id}")
        return VectorRecord.model_validate(r.json())

    def delete(self, id: int) -> None:
        """Delete a vector by ID."""
        self._http.delete(f"/collections/{self._name}/vectors/{id}")

    def search(
        self,
        vector: list[float],
        k: int,
        filter: Any | None = None,
        include_payload: bool = False,
        allowed_teams: list[str] | None = None,
        query_text: str | None = None,
    ) -> list[SearchResult]:
        """k-nearest-neighbour search with optional metadata filter.

        When the server runs with the enriched-search pipeline, returns
        ``list[PipelineResult]``; otherwise returns ``list[ScoredResult]``.
        """
        req = QueryRequest(
            vector=vector,
            k=k,
            filter=filter,
            include_payload=include_payload,
            allowed_teams=allowed_teams or [],
            query_text=query_text,
        )
        r = self._http.post(
            f"/collections/{self._name}/query",
            json=req.model_dump(exclude_none=True),
        )
        return _parse_search_results(r.json()["results"])

    def hybrid_search(
        self,
        vector: list[float],
        text: str,
        k: int,
        rrf_k: int = 60,
        filter: Any | None = None,
        include_payload: bool = False,
        allowed_teams: list[str] | None = None,
    ) -> list[SearchResult]:
        """Hybrid vector + BM25 search fused via Reciprocal Rank Fusion."""
        req = HybridQueryRequest(
            vector=vector,
            text=text,
            k=k,
            rrf_k=rrf_k,
            filter=filter,
            include_payload=include_payload,
            allowed_teams=allowed_teams or [],
        )
        r = self._http.post(
            f"/collections/{self._name}/hybrid-query",
            json=req.model_dump(exclude_none=True),
        )
        return _parse_search_results(r.json()["results"])

    def import_parquet(
        self,
        path: str,
        id_col: str,
        vector_col: str,
        payload_cols: list[str] | None = None,
    ) -> int:
        """Bulk-import vectors from a Parquet file. Returns the number imported."""
        req = ImportParquetRequest(
            path=path,
            id_col=id_col,
            vector_col=vector_col,
            payload_cols=payload_cols or [],
        )
        r = self._http.post(
            f"/collections/{self._name}/import-parquet",
            json=req.model_dump(),
        )
        return int(r.json()["imported"])

    def export_parquet(self, path: str) -> None:
        """Export the collection to a Parquet file at the given server-side path."""
        req = ExportParquetRequest(path=path)
        self._http.post(
            f"/collections/{self._name}/export-parquet",
            json=req.model_dump(),
        )


class AsyncCollection:
    """Async handle to a single LikhaDB collection."""

    def __init__(self, name: str, http: AsyncHttpClient) -> None:
        self._name = name
        self._http = http

    @property
    def name(self) -> str:
        return self._name

    async def info(self) -> CollectionInfo:
        r = await self._http.get(f"/collections/{self._name}")
        return CollectionInfo.model_validate(r.json())

    async def insert(
        self,
        id: int,
        vector: list[float],
        payload: dict[str, Any] | None = None,
    ) -> None:
        req = InsertRequest(id=id, vector=vector, payload=payload)
        await self._http.post(
            f"/collections/{self._name}/vectors",
            json=req.model_dump(exclude_none=True),
        )

    async def get(self, id: int) -> VectorRecord:
        r = await self._http.get(f"/collections/{self._name}/vectors/{id}")
        return VectorRecord.model_validate(r.json())

    async def delete(self, id: int) -> None:
        await self._http.delete(f"/collections/{self._name}/vectors/{id}")

    async def search(
        self,
        vector: list[float],
        k: int,
        filter: Any | None = None,
        include_payload: bool = False,
        allowed_teams: list[str] | None = None,
        query_text: str | None = None,
    ) -> list[SearchResult]:
        req = QueryRequest(
            vector=vector,
            k=k,
            filter=filter,
            include_payload=include_payload,
            allowed_teams=allowed_teams or [],
            query_text=query_text,
        )
        r = await self._http.post(
            f"/collections/{self._name}/query",
            json=req.model_dump(exclude_none=True),
        )
        return _parse_search_results(r.json()["results"])

    async def hybrid_search(
        self,
        vector: list[float],
        text: str,
        k: int,
        rrf_k: int = 60,
        filter: Any | None = None,
        include_payload: bool = False,
        allowed_teams: list[str] | None = None,
    ) -> list[SearchResult]:
        req = HybridQueryRequest(
            vector=vector,
            text=text,
            k=k,
            rrf_k=rrf_k,
            filter=filter,
            include_payload=include_payload,
            allowed_teams=allowed_teams or [],
        )
        r = await self._http.post(
            f"/collections/{self._name}/hybrid-query",
            json=req.model_dump(exclude_none=True),
        )
        return _parse_search_results(r.json()["results"])

    async def import_parquet(
        self,
        path: str,
        id_col: str,
        vector_col: str,
        payload_cols: list[str] | None = None,
    ) -> int:
        req = ImportParquetRequest(
            path=path,
            id_col=id_col,
            vector_col=vector_col,
            payload_cols=payload_cols or [],
        )
        r = await self._http.post(
            f"/collections/{self._name}/import-parquet",
            json=req.model_dump(),
        )
        return int(r.json()["imported"])

    async def export_parquet(self, path: str) -> None:
        req = ExportParquetRequest(path=path)
        await self._http.post(
            f"/collections/{self._name}/export-parquet",
            json=req.model_dump(),
        )
