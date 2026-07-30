"""Persist resumable migration phase and cursor state in PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from perfcho.infra.db.models.system import MaintenanceState


@dataclass(frozen=True, slots=True)
class MigrationCheckpoint:
    """Describe the latest committed migration phase and source cursor."""

    source_fingerprint: str
    config_digest: str
    phase: str
    cursor: int
    completed_phases: tuple[str, ...]
    status: str
    started_at: str


class MigrationStateStore:
    """Read and update one migration's resumable maintenance state."""

    def __init__(self, migration_id: str, session_factory: async_sessionmaker[AsyncSession]) -> None:
        """Bind a stable task key and session factory."""
        self.task = f"bancho-migration:{migration_id}"
        self._session_factory = session_factory

    async def load(self) -> MigrationCheckpoint | None:
        """Return the persisted checkpoint, if this migration was started."""
        async with self._session_factory() as session:
            state = await session.scalar(select(MaintenanceState.state).where(MaintenanceState.task == self.task))
        if state is None:
            return None
        return _decode(state)

    async def initialize(self, *, source_fingerprint: str, config_digest: str, started_at: datetime) -> None:
        """Create a pending checkpoint, preserving an existing compatible run."""
        current = await self.load()
        if current is not None:
            if current.source_fingerprint != source_fingerprint or current.config_digest != config_digest:
                raise RuntimeError("persisted migration state does not match the source or configuration")
            return
        async with self._session_factory.begin() as session:
            await self.save(
                session,
                MigrationCheckpoint(
                    source_fingerprint=source_fingerprint,
                    config_digest=config_digest,
                    phase="pending",
                    cursor=0,
                    completed_phases=(),
                    status="running",
                    started_at=started_at.astimezone(UTC).isoformat(),
                ),
            )

    async def save(self, session: AsyncSession, checkpoint: MigrationCheckpoint) -> None:
        """Upsert a checkpoint in the caller-owned business batch transaction."""
        await session.execute(
            insert(MaintenanceState)
            .values(task=self.task, state=_encode(checkpoint))
            .on_conflict_do_update(
                index_elements=(MaintenanceState.task,),
                set_={"state": _encode(checkpoint), "updated_at": datetime.now(UTC)},
            )
        )

    async def mark_completed(self) -> None:
        """Mark a fully verified migration as completed."""
        current = await self.load()
        if current is None:
            raise RuntimeError("migration state is not initialized")
        async with self._session_factory.begin() as session:
            await self.save(
                session,
                MigrationCheckpoint(
                    source_fingerprint=current.source_fingerprint,
                    config_digest=current.config_digest,
                    phase="completed",
                    cursor=0,
                    completed_phases=current.completed_phases,
                    status="completed",
                    started_at=current.started_at,
                ),
            )


def next_checkpoint(
    current: MigrationCheckpoint,
    *,
    phase: str,
    cursor: int,
    phase_completed: bool = False,
) -> MigrationCheckpoint:
    """Advance one checkpoint without losing source identity or completed phases."""
    completed = current.completed_phases
    if phase_completed and phase not in completed:
        completed = (*completed, phase)
    return MigrationCheckpoint(
        source_fingerprint=current.source_fingerprint,
        config_digest=current.config_digest,
        phase=phase,
        cursor=cursor,
        completed_phases=completed,
        status="running",
        started_at=current.started_at,
    )


def _encode(value: MigrationCheckpoint) -> dict[str, object]:
    return {
        "source_fingerprint": value.source_fingerprint,
        "config_digest": value.config_digest,
        "phase": value.phase,
        "cursor": value.cursor,
        "completed_phases": list(value.completed_phases),
        "status": value.status,
        "started_at": value.started_at,
    }


def _decode(value: dict[str, object]) -> MigrationCheckpoint:
    completed = value.get("completed_phases", [])
    if not isinstance(completed, list) or not all(isinstance(item, str) for item in completed):
        raise RuntimeError("migration checkpoint contains invalid completed phases")
    raw_cursor = value["cursor"]
    if isinstance(raw_cursor, bool) or not isinstance(raw_cursor, int | str):
        raise RuntimeError("migration checkpoint contains an invalid cursor")
    return MigrationCheckpoint(
        source_fingerprint=str(value["source_fingerprint"]),
        config_digest=str(value["config_digest"]),
        phase=str(value["phase"]),
        cursor=int(raw_cursor),
        completed_phases=tuple(completed),
        status=str(value["status"]),
        started_at=str(value["started_at"]),
    )
