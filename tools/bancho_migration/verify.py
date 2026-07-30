"""Run non-mutating preflight checks and post-migration reconciliation."""

from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from perfcho.infra.db.models.content import BeatmapRevision
from perfcho.infra.db.models.core import Account, AccountEmail, AccountName, MediaAsset
from perfcho.infra.db.models.scoring import PlayAttempt, Replay, Score, ScoreAttestation, ScoreHitStatistic
from perfcho.modules.common import ObjectStorage, ObjectUnavailable
from perfcho.modules.common.normalization import normalize_email, normalize_stable_name
from tools.bancho_migration.config import MigrationConfig
from tools.bancho_migration.models import DiagnosticSeverity, MigrationRuntime, SourceSchema
from tools.bancho_migration.report import MigrationReport
from tools.bancho_migration.schema import validate_source_schema
from tools.bancho_migration.source import BanchoSource
from tools.bancho_migration.storage import MigrationStorageError, read_beatmap_file, read_replay_file


async def preflight(
    config: MigrationConfig,
    source: BanchoSource,
    engine: AsyncEngine,
    report: MigrationReport,
) -> SourceSchema:
    """Validate source contracts, merge identities, files, and target connectivity without writes."""
    schema = source.inspect_schema()
    report.source_fingerprint = schema.fingerprint
    validate_source_schema(schema, report)
    if schema.version != "5.2.2":
        report.add(
            DiagnosticSeverity.WARNING,
            "source_version_not_baseline",
            "source startup version differs from the v5.2.2 baseline; compatible columns will still be used",
            details={"detected_version": schema.version or "unknown"},
        )
    await _target_connectivity(engine, report)
    await _account_preflight(config, source, engine, report)
    _content_identity_preflight(config, source, report)
    _asset_preflight(config.data_directory, source, report)
    return schema


async def reconcile(runtime: MigrationRuntime) -> None:
    """Check imported dependency graphs and source-to-target coverage after all phases."""
    async with runtime.session_factory() as session:
        prefix = f"bancho:{runtime.config.migration_id}:score:%"
        imported_scores = await session.scalar(
            select(func.count())
            .select_from(Score)
            .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
            .where(PlayAttempt.idempotency_key.like(prefix))
        )
        missing_attestations = await session.scalar(
            select(func.count())
            .select_from(Score)
            .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
            .outerjoin(ScoreAttestation, ScoreAttestation.score_id == Score.id)
            .where(PlayAttempt.idempotency_key.like(prefix), ScoreAttestation.score_id.is_(None))
        )
        scores_without_hits = await session.scalar(
            select(func.count())
            .select_from(Score)
            .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
            .outerjoin(ScoreHitStatistic, ScoreHitStatistic.score_id == Score.id)
            .where(PlayAttempt.idempotency_key.like(prefix), ScoreHitStatistic.score_id.is_(None))
        )
        duplicate_md5 = await session.scalar(
            select(func.count()).select_from(
                select(BeatmapRevision.md5).group_by(BeatmapRevision.md5).having(func.count() > 1).subquery()
            )
        )
    runtime.report.increment("verify", "imported_scores", int(imported_scores or 0))
    for code, count, message in (
        ("score_attestation_missing", missing_attestations, "imported scores are missing attestations"),
        ("score_hit_statistics_missing", scores_without_hits, "imported scores are missing hit statistics"),
        ("beatmap_md5_duplicate", duplicate_md5, "target beatmap revision MD5 values are not unique"),
    ):
        if count:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                code,
                message,
                details={"count": int(count)},
            )
    expected_accounts = runtime.source_schema.row_counts.get("users", 0)
    skipped_accounts = runtime.report.counters.get("identity.users", {}).get("skipped_override", 0)
    if len(runtime.mappings.accounts) + skipped_accounts < expected_accounts:
        runtime.report.add(
            DiagnosticSeverity.ERROR,
            "account_coverage_incomplete",
            "not every source account was mapped or explicitly skipped",
            details={
                "source": expected_accounts,
                "mapped": len(runtime.mappings.accounts),
                "explicitly_skipped": skipped_accounts,
            },
        )


async def verify_completed_run(
    config: MigrationConfig,
    source: BanchoSource,
    session_factory: async_sessionmaker[AsyncSession],
    report: MigrationReport,
    object_storage: ObjectStorage,
) -> None:
    """Verify durable counts for a completed run without reconstructing all mappings."""
    schema = source.inspect_schema()
    report.source_fingerprint = schema.fingerprint
    async with session_factory() as session:
        account_count = await session.scalar(select(func.count()).select_from(Account))
        score_count = await session.scalar(
            select(func.count())
            .select_from(PlayAttempt)
            .where(PlayAttempt.idempotency_key.like(f"bancho:{config.migration_id}:score:%"))
        )
    report.increment("verify", "target_accounts", int(account_count or 0))
    report.increment("verify", "imported_scores", int(score_count or 0))
    if int(account_count or 0) < schema.row_counts.get("users", 0):
        report.add(
            DiagnosticSeverity.WARNING,
            "target_account_count_lower_than_source",
            "target has fewer accounts than the source; inspect the original migration report for skips",
        )
    await _verify_beatmap_objects(config, source, session_factory, report, object_storage)
    await _verify_replay_objects(config, source, session_factory, report, object_storage)


async def _target_connectivity(engine: AsyncEngine, report: MigrationReport) -> None:
    try:
        async with engine.connect() as connection:
            version = await connection.scalar(text("SELECT current_setting('server_version_num')"))
        report.increment("preflight", "target_connected")
        if version is not None and int(version) < 170000:
            report.add(
                DiagnosticSeverity.WARNING,
                "target_postgresql_version",
                "the project baseline is PostgreSQL 17 or newer",
                details={"server_version_num": str(version)},
            )
    except Exception as error:
        report.add(DiagnosticSeverity.ERROR, "target_unavailable", str(error))


async def _account_preflight(
    config: MigrationConfig,
    source: BanchoSource,
    engine: AsyncEngine,
    report: MigrationReport,
) -> None:
    target_names: dict[str, int] = {}
    target_emails: dict[str, int] = {}
    target_ids: set[int] = set()
    try:
        async with engine.connect() as connection:
            target_ids = set((await connection.execute(select(Account.id))).scalars().all())
            target_names = dict(
                (
                    await connection.execute(
                        select(AccountName.name_key, AccountName.account_id).where(AccountName.ended_at.is_(None))
                    )
                ).all()
            )
            target_emails = dict(
                (
                    await connection.execute(
                        select(AccountEmail.email_key, AccountEmail.account_id).where(AccountEmail.retired_at.is_(None))
                    )
                ).all()
            )
    except SQLAlchemyError as error:
        report.add(
            DiagnosticSeverity.WARNING,
            "target_identity_unavailable",
            "target identity tables could not be inspected; apply will create missing tables",
            details={"error": str(error)},
        )

    seen_names: dict[str, int] = {}
    seen_emails: dict[str, int] = {}
    overrides = config.overrides.accounts
    for rows in source.iter_batches("users", key="id", batch_size=config.batch_size, columns=("id", "name", "email")):
        for row in rows:
            source_id = int(row["id"])
            override = overrides.get(source_id)
            if override is not None and override.skip:
                continue
            try:
                raw_name = override.display_name if override is not None and override.display_name else row["name"]
                raw_email = override.email if override is not None and override.email else row["email"]
                name_key = normalize_stable_name(str(raw_name).strip())
                email_key = normalize_email(str(raw_email))
            except ValueError as error:
                report.add(
                    DiagnosticSeverity.ERROR,
                    "account_identifier_invalid",
                    str(error),
                    entity="users",
                    source_id=source_id,
                )
                continue
            for seen, key, code in (
                (seen_names, name_key, "source_name_normalization_collision"),
                (seen_emails, email_key, "source_email_normalization_collision"),
            ):
                previous = seen.setdefault(key, source_id)
                if previous != source_id:
                    report.add(
                        DiagnosticSeverity.ERROR,
                        code,
                        "multiple source accounts normalize to one canonical identifier",
                        entity="users",
                        source_id=source_id,
                        details={"other_source_id": previous},
                    )
            if override is not None and override.target_account_id is not None:
                if override.target_account_id not in target_ids:
                    report.add(
                        DiagnosticSeverity.ERROR,
                        "account_override_missing",
                        "account override references a missing target account",
                        entity="users",
                        source_id=source_id,
                    )
                continue
            name_target = target_names.get(name_key)
            email_target = target_emails.get(email_key)
            if name_target is not None and email_target is not None and name_target != email_target:
                report.add(
                    DiagnosticSeverity.ERROR,
                    "account_resolution_ambiguous",
                    "source name and email belong to different target accounts",
                    entity="users",
                    source_id=source_id,
                    details={"name_target_id": name_target, "email_target_id": email_target},
                )
            id_target = source_id if source_id in target_ids else None
            natural_target = name_target or email_target
            if (
                id_target is not None
                and natural_target is not None
                and id_target != natural_target
                and not (name_target is not None and name_target == email_target)
            ):
                report.add(
                    DiagnosticSeverity.ERROR,
                    "account_resolution_ambiguous",
                    "source ID and one natural identifier belong to different target accounts",
                    entity="users",
                    source_id=source_id,
                )


def _asset_preflight(data_directory: Path, source: BanchoSource, report: MigrationReport) -> None:
    checked = 0
    for rows in source.iter_batches(
        "maps",
        key="id",
        batch_size=1000,
        columns=("id", "md5"),
    ):
        for row in rows:
            try:
                read_beatmap_file(data_directory, int(row["id"]), row["md5"])
                checked += 1
            except (MigrationStorageError, TypeError, ValueError) as error:
                report.add(
                    DiagnosticSeverity.WARNING,
                    "beatmap_file_unavailable",
                    str(error),
                    entity="maps",
                    source_id=row.get("id"),
                )
    report.increment("preflight", "beatmap_files_verified", checked)


def _content_identity_preflight(
    config: MigrationConfig,
    source: BanchoSource,
    report: MigrationReport,
) -> None:
    for table in ("mapsets", "maps"):
        identities: dict[int, str] = {}
        for rows in source.iter_batches(
            table,
            key="id",
            batch_size=config.batch_size,
            columns=("id", "server"),
        ):
            for row in rows:
                source_id = int(row["id"])
                server = str(row["server"])
                previous_server = identities.setdefault(source_id, server)
                if previous_server != server:
                    report.add(
                        DiagnosticSeverity.ERROR,
                        "content_source_id_ambiguous",
                        "the same legacy content ID exists in multiple source namespaces",
                        entity=table,
                        source_id=source_id,
                        details={"servers": sorted((previous_server, server))},
                    )


async def _verify_beatmap_objects(
    config: MigrationConfig,
    source: BanchoSource,
    session_factory: async_sessionmaker[AsyncSession],
    report: MigrationReport,
    object_storage: ObjectStorage,
) -> None:
    for rows in source.iter_batches(
        "maps",
        key="id",
        batch_size=config.batch_size,
        columns=("id", "md5"),
    ):
        expected: dict[bytes, tuple[int, bytes]] = {}
        for row in rows:
            try:
                metadata = read_beatmap_file(config.data_directory, int(row["id"]), row["md5"])
            except MigrationStorageError, TypeError, ValueError:
                continue
            expected[metadata.md5] = (int(row["id"]), metadata.sha256)
        if not expected:
            continue
        async with session_factory() as session:
            manifests = (
                await session.execute(
                    select(
                        BeatmapRevision.md5,
                        MediaAsset.storage_key,
                        MediaAsset.sha256,
                        MediaAsset.size_bytes,
                    )
                    .join(MediaAsset, MediaAsset.id == BeatmapRevision.file_asset_id)
                    .where(BeatmapRevision.md5.in_(expected))
                )
            ).all()
        by_md5 = {row.md5: row for row in manifests}
        for md5, (source_id, sha256) in expected.items():
            manifest = by_md5.get(md5)
            if manifest is None:
                report.add(
                    DiagnosticSeverity.ERROR,
                    "beatmap_object_manifest_missing",
                    "verified source beatmap has no target object manifest",
                    entity="maps",
                    source_id=source_id,
                )
                continue
            await _verify_object(
                object_storage,
                manifest.storage_key,
                expected_sha256=sha256,
                expected_size=manifest.size_bytes,
                report=report,
                entity="maps",
                source_id=source_id,
            )


async def _verify_replay_objects(
    config: MigrationConfig,
    source: BanchoSource,
    session_factory: async_sessionmaker[AsyncSession],
    report: MigrationReport,
    object_storage: ObjectStorage,
) -> None:
    for rows in source.iter_batches(
        "scores",
        key="id",
        batch_size=config.batch_size,
        columns=("id", "status"),
    ):
        expected: dict[object, tuple[int, bytes, int]] = {}
        for row in rows:
            try:
                source_id = int(row["id"])
                if int(row["status"]) == 0:
                    continue
                metadata = read_replay_file(config.data_directory, source_id)
            except MigrationStorageError, TypeError, ValueError:
                continue
            expected_key = f"bancho:{config.migration_id}:score:{source_id}"
            expected[expected_key] = (source_id, metadata.sha256, metadata.size_bytes)
        if not expected:
            continue
        async with session_factory() as session:
            manifests = (
                await session.execute(
                    select(
                        PlayAttempt.idempotency_key,
                        Replay.storage_key,
                        Replay.sha256,
                        Replay.size_bytes,
                    )
                    .join(Score, Score.attempt_id == PlayAttempt.id)
                    .join(Replay, Replay.score_id == Score.id)
                    .where(PlayAttempt.idempotency_key.in_(expected))
                )
            ).all()
        by_key = {row.idempotency_key: row for row in manifests}
        for key, (source_id, sha256, size_bytes) in expected.items():
            manifest = by_key.get(key)
            if manifest is None:
                report.add(
                    DiagnosticSeverity.ERROR,
                    "replay_object_manifest_missing",
                    "available source replay has no target object manifest",
                    entity="scores",
                    source_id=source_id,
                )
                continue
            if manifest.sha256 != sha256 or manifest.size_bytes != size_bytes:
                report.add(
                    DiagnosticSeverity.ERROR,
                    "replay_manifest_mismatch",
                    "target replay manifest disagrees with the source replay",
                    entity="scores",
                    source_id=source_id,
                )
                continue
            await _verify_object(
                object_storage,
                manifest.storage_key,
                expected_sha256=sha256,
                expected_size=size_bytes,
                report=report,
                entity="scores",
                source_id=source_id,
            )


async def _verify_object(
    object_storage: ObjectStorage,
    storage_key: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
    report: MigrationReport,
    entity: str,
    source_id: object,
) -> None:
    digest = hashlib.sha256()
    size = 0
    try:
        async with object_storage.open(storage_key) as stream:
            async for chunk in stream.iter_chunks():
                digest.update(chunk)
                size += len(chunk)
    except ObjectUnavailable as error:
        report.add(
            DiagnosticSeverity.ERROR,
            "object_storage_read_failed",
            str(error),
            entity=entity,
            source_id=source_id,
            details={"storage_key": storage_key},
        )
        return
    if digest.digest() != expected_sha256 or size != expected_size:
        report.add(
            DiagnosticSeverity.ERROR,
            "object_storage_content_mismatch",
            "stored object bytes disagree with the target manifest",
            entity=entity,
            source_id=source_id,
            details={"storage_key": storage_key, "size_bytes": size},
        )
    else:
        report.increment("verify", "objects_verified")
