"""Map moderation cases, sanctions, and anticheat findings."""

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import SanctionKind, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, Uuid7PrimaryKeyMixin


class ModerationCase(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Groups an account investigation, review, and appeal into one moderation case."""

    __tablename__ = "cases"
    __table_args__ = (
        Index("ix_cases_subject_status", "subject_account_id", "status"),
        Index("ix_cases_status_created", "status", "created_at"),
        {"schema": "moderation"},
    )

    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    opened_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    summary: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))


class CaseEntry(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores notes, evidence, and public explanations attached to moderation cases."""

    __tablename__ = "case_entries"
    __table_args__ = (Index("ix_case_entries_case_created", "case_id", "created_at"), {"schema": "moderation"})

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.cases.id", ondelete="RESTRICT"), nullable=False)
    author_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False, default="staff", server_default="staff")
    content: Mapped[str] = mapped_column(Text, nullable=False)
    evidence: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class Sanction(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores active and historical restrictions, silences, bans, and ranking freezes."""

    __tablename__ = "sanctions"
    __table_args__ = (
        CheckConstraint("ends_at IS NULL OR ends_at > starts_at", name="valid_period"),
        CheckConstraint("num_nonnulls(channel_id, team_id) <= 1", name="single_scope_target"),
        Index(
            "ix_sanctions_subject_active",
            "subject_account_id",
            "kind",
            "ends_at",
            postgresql_where=text("revoked_at IS NULL"),
        ),
        {"schema": "moderation"},
    )

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.cases.id", ondelete="RESTRICT"), nullable=False)
    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[SanctionKind] = mapped_column(enum_type(SanctionKind, "sanction_kind", 24), nullable=False)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey("community.channels.id", ondelete="RESTRICT"), nullable=True
    )
    team_id: Mapped[int | None] = mapped_column(ForeignKey("social.teams.id", ondelete="RESTRICT"), nullable=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    imposed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))


class SanctionEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records imposition, extension, revocation, and appeal events for sanctions."""

    __tablename__ = "sanction_events"
    __table_args__ = (
        Index("ix_sanction_events_sanction_created", "sanction_id", "created_at"),
        {"schema": "moderation"},
    )

    sanction_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.sanctions.id", ondelete="RESTRICT"), nullable=False
    )
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    details: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class AnticheatDetector(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines reproducible anti-cheat detector versions and configurations."""

    __tablename__ = "anticheat_detectors"
    __table_args__ = (UniqueConstraint("code", "version", "artifact_digest"), {"schema": "moderation"})

    code: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_digest: Mapped[bytes] = mapped_column(nullable=False)
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    active: Mapped[bool] = mapped_column(nullable=False, default=True, server_default="true")


class AnticheatRun(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks execution of a detector against a score."""

    __tablename__ = "anticheat_runs"
    __table_args__ = (
        UniqueConstraint("score_id", "detector_id"),
        Index("ix_anticheat_runs_status_created", "status", "created_at"),
        {"schema": "moderation"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), nullable=False)
    detector_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_detectors.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)


class AnticheatFinding(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores structured findings and evidence produced by anti-cheat detectors."""

    __tablename__ = "anticheat_findings"
    __table_args__ = (
        CheckConstraint("severity BETWEEN 0 AND 100", name="severity_range"),
        Index("ix_anticheat_findings_score_state", "score_id", "state", "severity"),
        {"schema": "moderation"},
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_runs.id", ondelete="RESTRICT"), nullable=False
    )
    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    features: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    evidence: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class CaseFinding(DbBase):
    """Associates moderation cases with anti-cheat findings."""

    __tablename__ = "case_findings"
    __table_args__ = ({"schema": "moderation"},)

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.cases.id", ondelete="RESTRICT"), primary_key=True)
    finding_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_findings.id", ondelete="RESTRICT"), primary_key=True
    )
