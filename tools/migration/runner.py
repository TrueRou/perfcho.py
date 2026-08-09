"""Compose preflight, domain migration, and reconciliation into one run."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.settings import settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.modules.common import ObjectUnavailable
from tools.migration.config import MigrationConfig
from tools.migration.domains.community import migrate_community
from tools.migration.domains.content import migrate_content
from tools.migration.domains.identity import migrate_identity
from tools.migration.domains.multiplayer import migrate_multiplayer
from tools.migration.domains.scoring import migrate_scoring
from tools.migration.domains.social import migrate_social
from tools.migration.models import DiagnosticSeverity, MigrationRuntime, MigrationStatus
from tools.migration.observability import event_fields
from tools.migration.report import MigrationReport
from tools.migration.source import BanchoSource
from tools.migration.state import MigrationStateStore
from tools.migration.target import (
    create_target_engine,
    create_target_session_factory,
    prepare_target,
    target_migration_lock,
)
from tools.migration.verify import preflight, reconcile, verify_completed_run

type _RunnerOperation = Callable[[MigrationReport], Awaitable[None]]


class MigrationRejected(RuntimeError):
    """Indicate a controlled migration precondition or reconciliation failure."""


async def run_preflight(config: MigrationConfig, *, invocation_id: str | None = None) -> MigrationReport:
    """Run compatibility checks plus a temporary S3 write/delete probe."""

    async def operation(report: MigrationReport) -> None:
        engine = create_target_engine(config.target_url)
        try:
            with BanchoSource(config.source_url) as source:
                await preflight(config, source, engine, report)
            await _probe_object_storage(config, report, S3ObjectStorage.from_settings(settings))
        finally:
            await engine.dispose()

    return await _run_runner(config, "preflight", operation, invocation_id=invocation_id)


async def run_migration(config: MigrationConfig, *, invocation_id: str | None = None) -> MigrationReport:
    """Apply a fully checked, resumable migration while both writers are offline."""

    async def operation(report: MigrationReport) -> None:
        if not config.confirm_offline:
            raise ValueError("apply requires --confirm-offline after stopping bancho and perfcho writers")
        engine = create_target_engine(config.target_url)
        try:
            object_storage = S3ObjectStorage.from_settings(settings)
            with BanchoSource(config.source_url) as source:
                source_schema = await preflight(config, source, engine, report)
            await _probe_object_storage(config, report, object_storage)
            report.write(config.report_path)
            if report.has_errors:
                raise MigrationRejected("migration preflight failed")

            async with target_migration_lock(
                engine,
                config.migration_id,
                invocation_id=report.invocation_id,
            ):
                prepare_started_ns = time.monotonic_ns()
                log_event("INFO", "migration.target.prepare_started", **event_fields(report))
                try:
                    session_factory = await prepare_target(engine)
                except BaseException as error:
                    log_event(
                        "WARNING" if isinstance(error, KeyboardInterrupt) else "ERROR",
                        "migration.target.prepare_failed",
                        exception=error,
                        error_type=type(error).__name__,
                        duration_ms=duration_ms(prepare_started_ns),
                        **event_fields(report),
                    )
                    raise
                log_event(
                    "INFO",
                    "migration.target.prepare_completed",
                    duration_ms=duration_ms(prepare_started_ns),
                    **event_fields(report),
                )

                state = MigrationStateStore(
                    config.migration_id,
                    session_factory,
                    invocation_id=report.invocation_id,
                )
                await state.initialize(
                    source_fingerprint=source_schema.fingerprint,
                    config_digest=config.digest,
                    started_at=report.started_at,
                )
                with BanchoSource(config.source_url) as source:
                    current_schema = source.inspect_schema()
                    if current_schema.fingerprint != source_schema.fingerprint:
                        raise MigrationRejected("source changed between preflight and apply")
                    runtime = MigrationRuntime(
                        config=config,
                        overrides=config.overrides,
                        source=source,
                        session_factory=session_factory,
                        state=state,
                        report=report,
                        source_schema=current_schema,
                        object_storage=object_storage,
                    )
                    await migrate_identity(runtime)
                    await migrate_social(runtime)
                    await migrate_community(runtime)
                    await migrate_content(runtime)
                    await migrate_scoring(runtime)
                    await migrate_multiplayer(runtime)
                    await reconcile(runtime)
                    if report.has_errors:
                        raise MigrationRejected("migration reconciliation failed")
                    await state.mark_completed()
        finally:
            await engine.dispose()

    return await _run_runner(config, "apply", operation, invocation_id=invocation_id)


async def _run_runner(
    config: MigrationConfig,
    command: str,
    operation: _RunnerOperation,
    *,
    invocation_id: str | None,
) -> MigrationReport:
    report = (
        MigrationReport(config.migration_id)
        if invocation_id is None
        else MigrationReport(config.migration_id, invocation_id=invocation_id)
    )
    started_ns = time.monotonic_ns()
    log_event("INFO", f"migration.{command}.started", command=command, **event_fields(report))
    try:
        await operation(report)
        report.finish()
        report.write(config.report_path)
    except KeyboardInterrupt as error:
        report.add(
            DiagnosticSeverity.ERROR,
            "migration_interrupted",
            "migration command was interrupted before completion",
            details={"command": command},
        )
        report.finish(MigrationStatus.INTERRUPTED)
        _write_failed_report(config, report)
        log_event(
            "WARNING",
            f"migration.{command}.interrupted",
            exception=error,
            command=command,
            status=report.status.value,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )
        _attach_report(error, report)
        raise
    except MigrationRejected as error:
        report.finish(MigrationStatus.FAILED)
        _write_failed_report(config, report)
        log_event(
            "ERROR",
            f"migration.{command}.failed",
            exception=error,
            command=command,
            status=report.status.value,
            failure="controlled_rejection",
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )
        _attach_report(error, report)
        raise
    except BaseException as error:
        report.add(
            DiagnosticSeverity.ERROR,
            "migration_fatal_exception",
            "migration command stopped because an unexpected exception was raised",
            details={"command": command, "error_type": type(error).__name__},
        )
        report.finish(MigrationStatus.FAILED)
        _write_failed_report(config, report)
        log_event(
            "ERROR",
            f"migration.{command}.failed",
            exception=error,
            command=command,
            status=report.status.value,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )
        _attach_report(error, report)
        raise
    terminal = "failed" if report.status is MigrationStatus.FAILED else "completed"
    log_event(
        "ERROR" if terminal == "failed" else "INFO",
        f"migration.{command}.{terminal}",
        command=command,
        status=report.status.value,
        diagnostics=sum(report.diagnostic_counts.values()),
        errors=report.diagnostic_counts[DiagnosticSeverity.ERROR.value],
        duration_ms=duration_ms(started_ns),
        **event_fields(report),
    )
    return report


def _write_failed_report(config: MigrationConfig, report: MigrationReport) -> None:
    try:
        report.write(config.report_path)
    except Exception as error:
        log_event(
            "ERROR",
            "migration.report.write_failed",
            exception=error,
            error_type=type(error).__name__,
            **event_fields(report),
        )


def _attach_report(error: BaseException, report: MigrationReport) -> None:
    error.__dict__["migration_report"] = report


async def _probe_object_storage(
    config: MigrationConfig,
    report: MigrationReport,
    object_storage: S3ObjectStorage,
) -> None:
    key = f"migration-probes/{config.migration_id}/{config.digest}.txt"
    payload = b"perfcho bancho migration storage probe\n"
    started_ns = time.monotonic_ns()
    log_event(
        "INFO",
        "migration.storage_probe.started",
        object_kind="probe",
        size_bytes=len(payload),
        **event_fields(report),
    )
    try:
        await object_storage.put(
            key,
            payload,
            media_type="text/plain",
        )
        await object_storage.delete(key)
    except ObjectUnavailable as error:
        report.add(
            DiagnosticSeverity.ERROR,
            "object_storage_unavailable",
            "object storage write/delete probe was unavailable",
            details={"error_type": type(error).__name__},
        )
        log_event(
            "ERROR",
            "migration.storage_probe.failed",
            exception=error,
            object_kind="probe",
            size_bytes=len(payload),
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )
    except BaseException as error:
        log_event(
            "WARNING" if isinstance(error, KeyboardInterrupt) else "ERROR",
            "migration.storage_probe.failed",
            exception=error,
            object_kind="probe",
            size_bytes=len(payload),
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )
        raise
    else:
        report.increment("preflight", "object_storage_write_delete_verified")
        log_event(
            "INFO",
            "migration.storage_probe.completed",
            object_kind="probe",
            size_bytes=len(payload),
            duration_ms=duration_ms(started_ns),
            **event_fields(report),
        )


async def run_verify(config: MigrationConfig, *, invocation_id: str | None = None) -> MigrationReport:
    """Recheck durable import counts for an already applied migration."""

    async def operation(report: MigrationReport) -> None:
        engine = create_target_engine(config.target_url)
        try:
            object_storage = S3ObjectStorage.from_settings(settings)
            await _probe_object_storage(config, report, object_storage)
            session_factory = create_target_session_factory(engine)
            state = MigrationStateStore(
                config.migration_id,
                session_factory,
                invocation_id=report.invocation_id,
            )
            checkpoint = await state.load()
            if checkpoint is None or checkpoint.status != "completed":
                raise MigrationRejected("migration verification requires a completed checkpoint")
            if checkpoint.config_digest != config.digest:
                raise MigrationRejected("migration verification configuration does not match the completed checkpoint")
            with BanchoSource(config.source_url) as source:
                source_schema = source.inspect_schema()
                if checkpoint.source_fingerprint != source_schema.fingerprint:
                    raise MigrationRejected("migration verification source does not match the completed checkpoint")
                await verify_completed_run(config, source, session_factory, report, object_storage)
        finally:
            await engine.dispose()

    return await _run_runner(config, "verify", operation, invocation_id=invocation_id)
