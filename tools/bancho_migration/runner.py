"""Compose preflight, domain migration, and reconciliation into one run."""

from __future__ import annotations

from perfcho.infra.s3 import S3ObjectStorage
from perfcho.infra.settings import settings
from perfcho.modules.common import ObjectUnavailable
from tools.bancho_migration.config import MigrationConfig
from tools.bancho_migration.domains.community import migrate_community
from tools.bancho_migration.domains.content import migrate_content
from tools.bancho_migration.domains.identity import migrate_identity
from tools.bancho_migration.domains.multiplayer import migrate_multiplayer
from tools.bancho_migration.domains.scoring import migrate_scoring
from tools.bancho_migration.domains.social import migrate_social
from tools.bancho_migration.models import DiagnosticSeverity, MigrationRuntime
from tools.bancho_migration.report import MigrationReport
from tools.bancho_migration.source import BanchoSource
from tools.bancho_migration.state import MigrationStateStore
from tools.bancho_migration.target import (
    create_target_engine,
    create_target_session_factory,
    prepare_target,
    target_migration_lock,
)
from tools.bancho_migration.verify import preflight, reconcile, verify_completed_run


async def run_preflight(config: MigrationConfig) -> MigrationReport:
    """Run compatibility checks plus a temporary S3 write/delete probe."""
    report = MigrationReport(config.migration_id)
    engine = create_target_engine(config.target_url)
    try:
        with BanchoSource(config.source_url) as source:
            await preflight(config, source, engine, report)
        await _probe_object_storage(config, report, S3ObjectStorage.from_settings(settings))
    except Exception:
        report.finish()
        report.write(config.report_path)
        raise
    finally:
        await engine.dispose()
    report.finish()
    report.write(config.report_path)
    return report


async def run_migration(config: MigrationConfig) -> MigrationReport:
    """Apply a fully checked, resumable migration while both writers are offline."""
    if not config.confirm_offline:
        raise ValueError("apply requires --confirm-offline after stopping bancho and perfcho writers")
    report = MigrationReport(config.migration_id)
    engine = create_target_engine(config.target_url)
    try:
        object_storage = S3ObjectStorage.from_settings(settings)
        with BanchoSource(config.source_url) as source:
            source_schema = await preflight(config, source, engine, report)
        await _probe_object_storage(config, report, object_storage)
        report.write(config.report_path)
        if report.has_errors:
            raise RuntimeError("migration preflight failed; inspect the JSON report")

        session_factory = await prepare_target(engine)
        state = MigrationStateStore(config.migration_id, session_factory)
        await state.initialize(
            source_fingerprint=source_schema.fingerprint,
            config_digest=config.digest,
            started_at=report.started_at,
        )
        async with target_migration_lock(engine, config.migration_id):
            with BanchoSource(config.source_url) as source:
                current_schema = source.inspect_schema()
                if current_schema.fingerprint != source_schema.fingerprint:
                    raise RuntimeError("source changed between preflight and apply")
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
                    raise RuntimeError("migration reconciliation failed; migration was not marked complete")
                await state.mark_completed()
    except Exception:
        report.finish()
        report.write(config.report_path)
        raise
    finally:
        await engine.dispose()
    report.finish()
    report.write(config.report_path)
    return report


async def _probe_object_storage(
    config: MigrationConfig,
    report: MigrationReport,
    object_storage: S3ObjectStorage,
) -> None:
    key = f"migration-probes/{config.migration_id}/{config.digest}.txt"
    payload = b"perfcho bancho migration storage probe\n"
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
            str(error),
        )
    else:
        report.increment("preflight", "object_storage_write_delete_verified")


async def run_verify(config: MigrationConfig) -> MigrationReport:
    """Recheck durable import counts for an already applied migration."""
    report = MigrationReport(config.migration_id)
    engine = create_target_engine(config.target_url)
    try:
        object_storage = S3ObjectStorage.from_settings(settings)
        await _probe_object_storage(config, report, object_storage)
        session_factory = create_target_session_factory(engine)
        with BanchoSource(config.source_url) as source:
            await verify_completed_run(config, source, session_factory, report, object_storage)
    except Exception:
        report.finish()
        report.write(config.report_path)
        raise
    finally:
        await engine.dispose()
    report.finish()
    report.write(config.report_path)
    return report
