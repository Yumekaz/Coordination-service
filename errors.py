"""
Domain-specific error types for the Coordination Service.
"""


class ConflictError(ValueError):
    """Raised when a valid request conflicts with current state."""

    def __init__(self, message: str, error: str = "conflict", **details):
        super().__init__(message)
        self.error = error
        self.details = details

    def to_response(self) -> dict:
        """Serialize the error for HTTP responses."""
        payload = {
            "error": self.error,
            "detail": str(self),
        }
        payload.update(self.details)
        return payload


class VersionConflictError(ConflictError):
    """Raised when a compare-and-swap write sees a stale version."""

    def __init__(self, path: str, expected_version: int, actual_version: int):
        super().__init__(
            f"Expected version {expected_version} but found {actual_version}",
            error="version_conflict",
            path=path,
            expected_version=expected_version,
            actual_version=actual_version,
        )


class ForbiddenError(PermissionError):
    """Raised when a caller is not allowed to perform an action."""

    def __init__(self, message: str, error: str = "forbidden", **details):
        super().__init__(message)
        self.error = error
        self.details = details

    def to_response(self) -> dict:
        """Serialize the error for HTTP responses."""
        payload = {
            "error": self.error,
            "detail": str(self),
        }
        payload.update(self.details)
        return payload
