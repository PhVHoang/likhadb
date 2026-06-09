"""likhadb — Python SDK for the LikhaDB vector database."""

from .client import AsyncLikhaDB, LikhaDB
from .collection import AsyncCollection, Collection
from .exceptions import (
    LikhaDBBadRequestError,
    LikhaDBConflictError,
    LikhaDBConnectionError,
    LikhaDBError,
    LikhaDBNotFoundError,
    LikhaDBServerError,
)
from .models import (
    CollectionInfo,
    FlatIndex,
    HnswIndex,
    IvfIndex,
    IvfSq8Index,
    PipelineResult,
    ScoredResult,
    VectorRecord,
)

__all__ = [
    # Clients
    "LikhaDB",
    "AsyncLikhaDB",
    # Collection handles
    "Collection",
    "AsyncCollection",
    # Response models
    "CollectionInfo",
    "PipelineResult",
    "ScoredResult",
    "VectorRecord",
    # Index config models
    "FlatIndex",
    "IvfIndex",
    "IvfSq8Index",
    "HnswIndex",
    # Exceptions
    "LikhaDBError",
    "LikhaDBConnectionError",
    "LikhaDBNotFoundError",
    "LikhaDBConflictError",
    "LikhaDBBadRequestError",
    "LikhaDBServerError",
]
