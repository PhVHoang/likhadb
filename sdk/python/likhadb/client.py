"""Top-level LikhaDB clients — sync (LikhaDB) and async (AsyncLikhaDB)."""
from __future__ import annotations

from typing import Literal, Optional

from ._http import AsyncHttpClient, HttpClient
from .collection import AsyncCollection, Collection
from .models import (
    CollectionInfo,
    CreateCollectionRequest,
    FlatIndex,
    HnswIndex,
    IvfIndex,
    IvfSq8Index,
)

_DEFAULT_URL = "http://localhost:8080"
_DEFAULT_TIMEOUT = 30.0


def _parse_index(index: Optional[dict]) -> object:
    if not index or index.get("type", "flat") == "flat":
        return FlatIndex()
    t = index["type"]
    if t == "ivf":
        return IvfIndex(nlist=index["nlist"], nprobe=index["nprobe"])
    if t == "ivf_sq8":
        return IvfSq8Index(nlist=index["nlist"], nprobe=index["nprobe"])
    if t == "hnsw":
        return HnswIndex(
            m=index["m"],
            ef_construction=index["ef_construction"],
            ef_search=index["ef_search"],
        )
    raise ValueError(f"Unknown index type {t!r}. Expected flat, ivf, ivf_sq8, or hnsw.")


class LikhaDB:
    """Synchronous client for the LikhaDB REST API.

    Usage::

        with LikhaDB("http://localhost:8080") as db:
            db.create_collection("docs", dim=384, metric="cosine")
            col = db.collection("docs")
            col.insert(1, vector=[0.1] * 384, payload={"title": "hello"})
            results = col.search([0.1] * 384, k=5, include_payload=True)
    """

    def __init__(
        self,
        url: str = _DEFAULT_URL,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self._http = HttpClient(base_url=url, timeout=timeout)

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "LikhaDB":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ── Health ───────────────────────────────────────────────────────────────

    def health(self) -> dict:
        """Return the server health payload."""
        return self._http.get("/health").json()

    # ── Collection DDL ───────────────────────────────────────────────────────

    def list_collections(self) -> list[str]:
        """Return the names of all collections."""
        return self._http.get("/collections").json()["collections"]

    def create_collection(
        self,
        name: str,
        dim: int,
        metric: Literal["l2", "cosine", "dot"] = "cosine",
        index: Optional[dict] = None,
        enable_fts: bool = False,
    ) -> None:
        """Create a new collection.

        Args:
            name: Collection name.
            dim: Vector dimensionality.
            metric: Distance metric — ``"l2"``, ``"cosine"``, or ``"dot"``.
            index: Index configuration dict.  ``None`` or omitted → flat exact search.

                   Examples::

                       {"type": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 50}
                       {"type": "ivf",  "nlist": 1024, "nprobe": 16}
                       {"type": "ivf_sq8", "nlist": 1024, "nprobe": 16}

            enable_fts: Activate Tantivy BM25 full-text index on payload string fields.
        """
        req = CreateCollectionRequest(
            name=name,
            dim=dim,
            metric=metric,
            index=_parse_index(index),
            enable_fts=enable_fts,
        )
        self._http.post("/collections", json=req.model_dump())

    def get_collection(self, name: str) -> CollectionInfo:
        """Return metadata for a collection."""
        r = self._http.get(f"/collections/{name}")
        return CollectionInfo.model_validate(r.json())

    def delete_collection(self, name: str) -> None:
        """Permanently delete a collection and all its vectors."""
        self._http.delete(f"/collections/{name}")

    def collection(self, name: str) -> Collection:
        """Return a :class:`Collection` handle for vector operations."""
        return Collection(name=name, http=self._http)


class AsyncLikhaDB:
    """Asynchronous client for the LikhaDB REST API.

    Usage::

        async with AsyncLikhaDB("http://localhost:8080") as db:
            await db.create_collection("docs", dim=384, metric="cosine")
            col = db.collection("docs")
            await col.insert(1, vector=[0.1] * 384, payload={"title": "hello"})
            results = await col.search([0.1] * 384, k=5, include_payload=True)
    """

    def __init__(
        self,
        url: str = _DEFAULT_URL,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self._http = AsyncHttpClient(base_url=url, timeout=timeout)

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncLikhaDB":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    # ── Health ───────────────────────────────────────────────────────────────

    async def health(self) -> dict:
        r = await self._http.get("/health")
        return r.json()

    # ── Collection DDL ───────────────────────────────────────────────────────

    async def list_collections(self) -> list[str]:
        r = await self._http.get("/collections")
        return r.json()["collections"]

    async def create_collection(
        self,
        name: str,
        dim: int,
        metric: Literal["l2", "cosine", "dot"] = "cosine",
        index: Optional[dict] = None,
        enable_fts: bool = False,
    ) -> None:
        req = CreateCollectionRequest(
            name=name,
            dim=dim,
            metric=metric,
            index=_parse_index(index),
            enable_fts=enable_fts,
        )
        await self._http.post("/collections", json=req.model_dump())

    async def get_collection(self, name: str) -> CollectionInfo:
        r = await self._http.get(f"/collections/{name}")
        return CollectionInfo.model_validate(r.json())

    async def delete_collection(self, name: str) -> None:
        await self._http.delete(f"/collections/{name}")

    def collection(self, name: str) -> AsyncCollection:
        """Return an :class:`AsyncCollection` handle for vector operations."""
        return AsyncCollection(name=name, http=self._http)
