"""likhadb — Python SDK for the LikhaDB vector database."""

from .client import AsyncLikhaDB, LikhaDB
from .collection import AsyncCollection, Collection
from .exceptions import (
    LikhaDBBadRequestError,
    LikhaDBConflictError,
    LikhaDBConnectionError,
    LikhaDBError,
    LikhaDBForbiddenError,
    LikhaDBNotFoundError,
    LikhaDBServerError,
    LikhaDBUnauthorizedError,
)
from .models import (
    CollectionInfo,
    FlatIndex,
    HnswIndex,
    IvfIndex,
    IvfSq8Index,
    PipelineResult,
    ScoredResult,
    SourceBinding,
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
    "SourceBinding",
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
    "LikhaDBUnauthorizedError",
    "LikhaDBForbiddenError",
    "LikhaDBServerError",
]
