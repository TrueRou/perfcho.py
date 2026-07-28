"""Import every canonical model so SQLAlchemy registers all tables."""

from . import (
    audit,
    authz,
    community,
    content,
    core,
    events,
    iam,
    moderation,
    multiplayer,
    scoring,
    social,
    system,
)

__all__ = [
    "audit",
    "authz",
    "community",
    "content",
    "core",
    "events",
    "iam",
    "moderation",
    "multiplayer",
    "scoring",
    "social",
    "system",
]
