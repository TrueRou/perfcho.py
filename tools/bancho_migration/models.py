"""Define migration state shared by source, domain, and verification modules."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from perfcho.modules.common.ports import ObjectStorage
    from tools.bancho_migration.config import MigrationConfig, MigrationOverrides
    from tools.bancho_migration.report import MigrationReport
    from tools.bancho_migration.source import BanchoSource
    from tools.bancho_migration.state import MigrationStateStore


class DiagnosticSeverity(StrEnum):
    """Classify migration diagnostics by whether execution may continue."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """Describe one source row or migration-wide problem without secrets."""

    severity: DiagnosticSeverity
    code: str
    message: str
    entity: str | None = None
    source_id: str | None = None
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SourceSchema:
    """Snapshot the detected legacy schema and source identity."""

    tables: dict[str, frozenset[str]]
    version: str | None
    row_counts: dict[str, int]
    fingerprint: str


@dataclass(slots=True)
class MigrationMappings:
    """Keep bounded source-to-target identities needed by later phases."""

    accounts: dict[int, int] = field(default_factory=dict)
    source_account_ids: set[int] = field(default_factory=set)
    beatmapsets: dict[int, int] = field(default_factory=dict)
    beatmaps: dict[int, int] = field(default_factory=dict)
    revisions_by_md5: dict[str, int] = field(default_factory=dict)
    teams: dict[int, int] = field(default_factory=dict)
    channels: dict[int, int] = field(default_factory=dict)
    achievements: dict[int, int] = field(default_factory=dict)
    scores: dict[int, int] = field(default_factory=dict)
    tournament_pools: dict[int, uuid.UUID] = field(default_factory=dict)
    calculation_releases: dict[tuple[str, str], uuid.UUID] = field(default_factory=dict)


class MigrationIds:
    """Generate deterministic UUIDs scoped to one reviewed migration ID."""

    def __init__(self, migration_id: str) -> None:
        """Derive a stable namespace without persisting source credentials."""
        self._namespace = uuid.uuid5(uuid.NAMESPACE_URL, f"https://perfcho.dev/bancho-migration/{migration_id}")

    def make(self, entity: str, source_id: object) -> uuid.UUID:
        """Return a deterministic UUID for one source entity."""
        return uuid.uuid5(self._namespace, f"{entity}:{source_id}")


@dataclass(slots=True)
class MigrationRuntime:
    """Bundle explicit migration dependencies and mutable identity mappings."""

    config: MigrationConfig
    overrides: MigrationOverrides
    source: BanchoSource
    session_factory: async_sessionmaker[AsyncSession]
    state: MigrationStateStore
    report: MigrationReport
    source_schema: SourceSchema
    object_storage: ObjectStorage | None = None
    mappings: MigrationMappings = field(default_factory=MigrationMappings)
    ids: MigrationIds = field(init=False)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Create the deterministic ID generator after configuration is available."""
        self.ids = MigrationIds(self.config.migration_id)


type SourceRow = dict[str, Any]
