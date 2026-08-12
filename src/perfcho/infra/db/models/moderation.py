"""Map moderation cases, sanctions, and anticheat findings."""

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    LargeBinary,
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

    __tablename__ = "case"
    __table_args__ = (
        Index("ix_case_subject_status", "subject_account_id", "status"),
        Index("ix_case_status_created", "status", "created_at"),
        {"schema": "moderation"},
    )

    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    opened_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    summary: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))


class CaseEntry(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores notes, evidence, and public explanations attached to moderation cases."""

    __tablename__ = "case_entry"
    __table_args__ = (Index("ix_case_entry_case_created", "case_id", "created_at"), {"schema": "moderation"})

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.case.id", ondelete="RESTRICT"), nullable=False)
    author_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False, default="staff", server_default="staff")
    content: Mapped[str] = mapped_column(Text, nullable=False)
    evidence: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class Sanction(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores active and historical restrictions, silences, bans, and ranking freezes."""

    __tablename__ = "sanction"
    __table_args__ = (
        CheckConstraint("ends_at IS NULL OR ends_at > starts_at", name="valid_period"),
        CheckConstraint("num_nonnulls(channel_id, team_id) <= 1", name="single_scope_target"),
        Index(
            "ix_sanction_subject_active",
            "subject_account_id",
            "kind",
            "ends_at",
            postgresql_where=text("revoked_at IS NULL"),
        ),
        {"schema": "moderation"},
    )

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.case.id", ondelete="RESTRICT"), nullable=False)
    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[SanctionKind] = mapped_column(enum_type(SanctionKind, "sanction_kind", 24), nullable=False)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey("community.channel.id", ondelete="RESTRICT"), nullable=True
    )
    team_id: Mapped[int | None] = mapped_column(ForeignKey("social.team.id", ondelete="RESTRICT"), nullable=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    imposed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))


class SanctionEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records imposition, extension, revocation, and appeal events for sanctions."""

    __tablename__ = "sanction_event"
    __table_args__ = (
        Index("ix_sanction_event_sanction_created", "sanction_id", "created_at"),
        {"schema": "moderation"},
    )

    sanction_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.sanction.id", ondelete="RESTRICT"), nullable=False
    )
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    details: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class AnticheatDetector(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines reproducible anti-cheat detector versions and configurations."""

    __tablename__ = "anticheat_detector"
    __table_args__ = (UniqueConstraint("code", "version", "artifact_digest"), {"schema": "moderation"})

    code: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_digest: Mapped[bytes] = mapped_column(nullable=False)
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    active: Mapped[bool] = mapped_column(nullable=False, default=True, server_default="true")


class AnticheatRun(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks execution of a detector against a score."""

    __tablename__ = "anticheat_run"
    __table_args__ = (
        UniqueConstraint("score_id", "detector_id"),
        Index("ix_anticheat_run_status_created", "status", "created_at"),
        {"schema": "moderation"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.score.id", ondelete="RESTRICT"), nullable=False)
    detector_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_detector.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)


class AnticheatFinding(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores structured findings and evidence produced by anti-cheat detectors."""

    __tablename__ = "anticheat_finding"
    __table_args__ = (
        CheckConstraint("severity BETWEEN 0 AND 100", name="severity_range"),
        CheckConstraint("octet_length(finding_digest) = 32", name="finding_digest_length"),
        CheckConstraint(
            "(reviewed_at IS NULL AND reviewed_by_id IS NULL AND review_outcome IS NULL AND review_notes IS NULL) OR "
            "(reviewed_at IS NOT NULL AND reviewed_by_id IS NOT NULL AND review_outcome IS NOT NULL)",
            name="complete_review",
        ),
        UniqueConstraint("run_id", "finding_digest", name="uq_anticheat_finding_run_digest"),
        Index("ix_anticheat_finding_score_state", "score_id", "state", "severity"),
        Index("ix_anticheat_finding_review_queue", "state", "severity", "created_at"),
        {"schema": "moderation"},
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_run.id", ondelete="RESTRICT"), nullable=False
    )
    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.score.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    finding_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    severity: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    features: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    evidence: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    reviewed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"))
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_outcome: Mapped[str | None] = mapped_column(String(16))
    review_notes: Mapped[str | None] = mapped_column(Text)


class CaseFinding(DbBase):
    """Associates moderation cases with anti-cheat findings."""

    __tablename__ = "case_finding"
    __table_args__ = ({"schema": "moderation"},)

    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("moderation.case.id", ondelete="RESTRICT"), primary_key=True)
    finding_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("moderation.anticheat_finding.id", ondelete="RESTRICT"), primary_key=True
    )
