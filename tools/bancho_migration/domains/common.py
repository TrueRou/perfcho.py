"""Provide transaction-bound phase execution shared by migration domains."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from tools.bancho_migration.models import MigrationRuntime, SourceRow
from tools.bancho_migration.observability import PhaseObserver
from tools.bancho_migration.state import MigrationCheckpoint, next_checkpoint

type BatchHandler = Callable[[AsyncSession, list[SourceRow]], Awaitable[None]]
type SingleHandler = Callable[[AsyncSession], Awaitable[None]]


async def run_batched_phase(
    runtime: MigrationRuntime,
    *,
    phase: str,
    table: str,
    key: str,
    handler: BatchHandler,
    columns: Sequence[str] = ("*",),
) -> None:
    """Run and checkpoint one key-ordered source table in target-owned transactions."""
    with PhaseObserver(runtime, phase) as observer:
        checkpoint = await _checkpoint(runtime)
        if phase in checkpoint.completed_phases:
            runtime.report.increment(phase, "resumed_complete", 0)
            observer.skipped()
            return
        cursor = checkpoint.cursor if checkpoint.phase == phase else 0
        for rows in runtime.source.iter_batches(
            table,
            key=key,
            batch_size=runtime.config.batch_size,
            start_after=cursor,
            columns=columns,
        ):
            snapshot = runtime.report.snapshot()
            try:
                async with runtime.session_factory.begin() as session:
                    await handler(session, rows)
                    cursor = int(rows[-1][key])
                    checkpoint = next_checkpoint(checkpoint, phase=phase, cursor=cursor)
                    await runtime.state.save(session, checkpoint)
            except BaseException:
                runtime.report.restore(snapshot)
                raise
            observer.batch_committed(len(rows))
            runtime.report.write(runtime.config.report_path)
        await complete_phase(runtime, checkpoint, phase)


async def run_single_phase(runtime: MigrationRuntime, *, phase: str, handler: SingleHandler) -> None:
    """Run one catalog or projection phase atomically and mark it completed."""
    with PhaseObserver(runtime, phase) as observer:
        checkpoint = await _checkpoint(runtime)
        if phase in checkpoint.completed_phases:
            observer.skipped()
            return
        snapshot = runtime.report.snapshot()
        try:
            async with runtime.session_factory.begin() as session:
                await handler(session)
                checkpoint = next_checkpoint(checkpoint, phase=phase, cursor=0, phase_completed=True)
                await runtime.state.save(session, checkpoint)
        except BaseException:
            runtime.report.restore(snapshot)
            raise
        runtime.report.write(runtime.config.report_path)


async def complete_phase(runtime: MigrationRuntime, checkpoint: MigrationCheckpoint, phase: str) -> None:
    """Mark a batch phase complete in its own short target transaction."""
    async with runtime.session_factory.begin() as session:
        await runtime.state.save(
            session,
            next_checkpoint(checkpoint, phase=phase, cursor=0, phase_completed=True),
        )
    runtime.report.write(runtime.config.report_path)


async def _checkpoint(runtime: MigrationRuntime) -> MigrationCheckpoint:
    checkpoint = await runtime.state.load()
    if checkpoint is None:
        raise RuntimeError("migration state is not initialized")
    return checkpoint
