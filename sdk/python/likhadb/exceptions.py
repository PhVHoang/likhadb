class LikhaDBError(Exception):
    """Base class for all LikhaDB SDK errors."""


class LikhaDBConnectionError(LikhaDBError):
    """Could not reach the LikhaDB server."""


class LikhaDBNotFoundError(LikhaDBError):
    """The requested collection or vector does not exist (404)."""


class LikhaDBConflictError(LikhaDBError):
    """A resource with that name already exists (409)."""


class LikhaDBBadRequestError(LikhaDBError):
    """The server rejected the request as invalid (400)."""


class LikhaDBServerError(LikhaDBError):
    """The server returned an unexpected error (5xx)."""
