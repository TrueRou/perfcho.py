"""Expose the protocol-neutral application kernel."""

from perfcho.modules.common.errors import (
    AccountUnavailable,
    ApplicationError,
    AuthenticationFailed,
    AuthorizationDenied,
    ConcurrentModification,
    DependencyUnavailable,
    IdempotencyConflict,
    InputRejected,
    ObjectUnavailable,
    ProjectionUnavailable,
    RateLimitExceeded,
    ResourceConflict,
    ResourceNotFound,
    SessionExpired,
    SessionRevoked,
)
from perfcho.modules.common.models import Actor, ClientContext, CommandMeta, JsonValue, PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator, OutboxWriter, UnitOfWork, UnitOfWorkFactory

__all__ = (
    "AccountUnavailable",
    "Actor",
    "ApplicationError",
    "AuthenticationFailed",
    "AuthorizationDenied",
    "ClientContext",
    "Clock",
    "CommandMeta",
    "ConcurrentModification",
    "DependencyUnavailable",
    "IdGenerator",
    "IdempotencyConflict",
    "InputRejected",
    "JsonValue",
    "ObjectUnavailable",
    "OutboxWriter",
    "PendingEvent",
    "ProjectionUnavailable",
    "RateLimitExceeded",
    "ResourceConflict",
    "ResourceNotFound",
    "SessionExpired",
    "SessionRevoked",
    "UnitOfWork",
    "UnitOfWorkFactory",
)
