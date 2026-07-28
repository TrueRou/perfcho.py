"""Define the common JSON response envelope for managed APIs."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class AppResponse[T](BaseModel):
    """Wrap API data with an application code, message, and timestamp."""

    data: T | None = None
    code: int = 200
    message: str = "success"
    timestamp: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


class ResponseHandler:
    """Construct consistently shaped success and error responses."""

    @staticmethod
    def success[T](data: T | None, message: str = "success") -> AppResponse[T]:
        """Return a successful response containing optional typed data."""
        return AppResponse(data=data, message=message)

    @staticmethod
    def error(message: str = "error", code: int = 500) -> AppResponse[None]:
        """Return an error response without exposing data."""
        return AppResponse(message=message, code=code)
