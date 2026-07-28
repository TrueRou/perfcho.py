from datetime import UTC, datetime

from pydantic import BaseModel, Field


class AppResponse[T](BaseModel):
    data: T | None = None
    code: int = 200
    message: str = "success"
    timestamp: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


class ResponseHandler:
    @staticmethod
    def success[T](data: T | None, message: str = "success") -> AppResponse[T]:
        return AppResponse(data=data, message=message)

    @staticmethod
    def error(message: str = "error", code: int = 500) -> AppResponse[None]:
        return AppResponse(message=message, code=code)
