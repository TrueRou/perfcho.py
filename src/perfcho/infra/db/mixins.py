"""Provide consistent identity and timestamp columns to mapped models."""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Identity, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column


class CreatedAtMixin:
    """Add an immutable server-generated creation timestamp."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class TimestampMixin(CreatedAtMixin):
    """Add creation and automatically maintained update timestamps."""

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class BigIntIdentityMixin:
    """Add a generated 64-bit append-oriented primary key."""

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)


class Uuid7PrimaryKeyMixin:
    """Add an application-generated time-ordered UUID primary key."""

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid7)
