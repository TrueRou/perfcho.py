"""Define protocol-neutral moderation commands and result values."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime

from perfcho.modules.common.models import CommandMeta, JsonValue


@dataclass(frozen=True, slots=True)
class OpenCase:
    """Open an investigation for one subject account."""

    meta: CommandMeta
    subject_account_id: int
    summary: str
    severity: int = 0


@dataclass(frozen=True, slots=True)
class AddCaseEntry:
    """Append a note, evidence record, or explanation to an open case."""

    meta: CommandMeta
    case_id: uuid.UUID
    kind: str
    content: str
    visibility: str = "staff"
    evidence: dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ImposeSanction:
    """Impose a scoped sanction for a case subject."""

    meta: CommandMeta
    case_id: uuid.UUID
    subject_account_id: int
    kind: str
    starts_at: datetime
    ends_at: datetime | None
    reason: str
    channel_id: int | None = None
    team_id: int | None = None


@dataclass(frozen=True, slots=True)
class ExtendSanction:
    """Extend an active sanction to a later end instant."""

    meta: CommandMeta
    sanction_id: uuid.UUID
    ends_at: datetime
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class RevokeSanction:
    """Revoke an imposed sanction."""

    meta: CommandMeta
    sanction_id: uuid.UUID
    reason: str


@dataclass(frozen=True, slots=True)
class ModerationCase:
    """Return the durable identity and state of a moderation case."""

    case_id: uuid.UUID
    subject_account_id: int
    status: str
    summary: str
    severity: int


@dataclass(frozen=True, slots=True)
class CaseEntry:
    """Return the durable identity of a case entry."""

    entry_id: int
    case_id: uuid.UUID
    author_account_id: int
    kind: str
    visibility: str
    content: str


@dataclass(frozen=True, slots=True)
class SanctionRecord:
    """Return the durable state of a sanction."""

    sanction_id: uuid.UUID
    case_id: uuid.UUID
    subject_account_id: int
    kind: str
    channel_id: int | None
    team_id: int | None
    starts_at: datetime
    ends_at: datetime | None
    reason: str
    revoked_at: datetime | None
