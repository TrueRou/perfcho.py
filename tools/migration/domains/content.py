"""Migrate bancho.py beatmap content and its directly-owned community facts."""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import BeatmapStatus, Ruleset
from perfcho.infra.db.models.content import (
    Beatmap,
    BeatmapRevision,
    Beatmapset,
    BeatmapsetFavourite,
    BeatmapStatusEvent,
    Comment,
    ContentSource,
    ContentSyncState,
    MapStatusRequest,
    RatingVote,
)
from perfcho.infra.db.models.core import MediaAsset
from perfcho.modules.common import StoredObject
from tools.migration.domains.common import complete_phase, run_batched_phase, run_single_phase
from tools.migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.migration.observability import PhaseObserver
from tools.migration.state import MigrationCheckpoint, next_checkpoint
from tools.migration.storage import (
    BeatmapFileMetadata,
    MigrationStorageError,
    ObjectUploadFailed,
    read_beatmap_file,
    upload_beatmap_file,
)
from tools.migration.transforms import (
    aware_datetime,
    beatmap_status,
    bounded_integer,
    decimal_value,
    source_ruleset,
    unix_datetime,
)

_PHASE_SOURCES = "content.sources"
_PHASE_MAPSETS = "content.mapsets"
_PHASE_MAPS = "content.maps"
_PHASE_FAVOURITES = "content.favourites"
_PHASE_RATINGS = "content.ratings"
_PHASE_MAP_REQUESTS = "content.map_requests"
_PHASE_COMMENTS = "content.comments"
_COLOR = re.compile(r"[0-9A-Fa-f]{6}")


@dataclass(frozen=True, slots=True)
class _SetMetadata:
    source_code: str
    external_id: int
    creator_name: str
    artist: str
    title: str
    status: BeatmapStatus
    last_source_update_at: datetime
    last_checked_at: datetime


@dataclass(frozen=True, slots=True)
class _StagedMap:
    source_id: int
    source_code: str
    external_id: int
    beatmapset_id: int
    status: BeatmapStatus
    ruleset: Ruleset
    difficulty_name: str
    status_locked: bool
    file_name: str
    source_updated_at: datetime
    total_length_ms: int
    bpm: Decimal
    circle_size: Decimal
    overall_difficulty: Decimal
    approach_rate: Decimal
    health_drain: Decimal
    max_combo: int
    file: BeatmapFileMetadata
    stored_object: StoredObject


type _RowsHandler = Callable[[AsyncSession, list[SourceRow]], Awaitable[None]]


async def migrate_content(runtime: MigrationRuntime) -> None:
    """Migrate content tables in dependency order with resumable checkpoints."""
    if runtime.object_storage is None:
        raise RuntimeError("content migration requires an injected ObjectStorage")
    await _migrate_sources(runtime)
    await _populate_content_mappings(runtime)
    await _migrate_mapsets(runtime)
    await _migrate_maps(runtime)
    await _populate_content_mappings(runtime)
    await _migrate_favourites(runtime)
    await _migrate_ratings(runtime)
    await _migrate_map_requests(runtime)
    await _migrate_comments(runtime)


async def _migrate_sources(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession) -> None:
        await session.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('content.source', 'id'), "
                "GREATEST(1, (SELECT COALESCE(MAX(id), 0) FROM content.source)), true)"
            )
        )
        statement = insert(ContentSource).values(
            code="private",
            name="Bancho private content",
            base_url=None,
            official=False,
        )
        await session.execute(statement.on_conflict_do_nothing(index_elements=(ContentSource.code,)))
        runtime.report.increment(_PHASE_SOURCES, "upserted")

    await run_single_phase(runtime, phase=_PHASE_SOURCES, handler=handler)


async def _migrate_mapsets(runtime: MigrationRuntime) -> None:
    with PhaseObserver(runtime, _PHASE_MAPSETS) as observer:
        checkpoint = await _checkpoint(runtime)
        if _PHASE_MAPSETS in checkpoint.completed_phases:
            runtime.report.increment(_PHASE_MAPSETS, "resumed_complete", 0)
            observer.skipped()
            return
        cursor = checkpoint.cursor if checkpoint.phase == _PHASE_MAPSETS else 0
        for rows in runtime.source.iter_batches(
            "mapsets",
            key="id",
            batch_size=runtime.config.batch_size,
            start_after=cursor,
        ):
            snapshot = runtime.report.snapshot()
            prepared: list[tuple[int, _SetMetadata]] = []
            for row in rows:
                source_set_id = _positive(row, "id")
                try:
                    server = _source_server(row["server"])
                    maps = runtime.source.fetch_all(
                        "maps",
                        where="`server` = %s AND `set_id` = %s",
                        parameters=(server, source_set_id),
                        order_by=("last_update DESC", "id DESC"),
                    )
                    prepared.append((source_set_id, _derive_set_metadata(runtime, row, maps)))
                except ValueError as error:
                    _diagnose(runtime, _PHASE_MAPSETS, "mapset_invalid", error, "mapset", source_set_id)

            mapped: list[tuple[int, int]] = []
            try:
                async with runtime.session_factory.begin() as session:
                    for source_set_id, metadata in prepared:
                        try:
                            async with session.begin_nested():
                                target_set_id, created = await _persist_mapset(session, metadata)
                        except IntegrityError as error:
                            _diagnose(
                                runtime,
                                _PHASE_MAPSETS,
                                "mapset_target_conflict",
                                error,
                                "mapset",
                                source_set_id,
                            )
                            continue
                        mapped.append((source_set_id, target_set_id))
                        runtime.report.increment(_PHASE_MAPSETS, "inserted" if created else "target_reused")
                    cursor = int(rows[-1]["id"])
                    checkpoint = next_checkpoint(checkpoint, phase=_PHASE_MAPSETS, cursor=cursor)
                    await runtime.state.save(session, checkpoint)
            except BaseException:
                runtime.report.restore(snapshot)
                raise
            runtime.mappings.beatmapsets.update(mapped)
            observer.batch_committed(len(rows))
            runtime.report.write(runtime.config.report_path)
        await complete_phase(runtime, checkpoint, _PHASE_MAPSETS)


async def _migrate_maps(runtime: MigrationRuntime) -> None:
    with PhaseObserver(runtime, _PHASE_MAPS) as observer:
        checkpoint = await _checkpoint(runtime)
        if _PHASE_MAPS in checkpoint.completed_phases:
            runtime.report.increment(_PHASE_MAPS, "resumed_complete", 0)
            observer.skipped()
            return
        cursor = checkpoint.cursor if checkpoint.phase == _PHASE_MAPS else 0
        object_storage = runtime.object_storage
        assert object_storage is not None
        for rows in runtime.source.iter_batches(
            "maps",
            key="id",
            batch_size=runtime.config.batch_size,
            start_after=cursor,
        ):
            snapshot = runtime.report.snapshot()
            staged: list[_StagedMap] = []
            for row in rows:
                source_map_id = _positive(row, "id")
                source_set_id = _positive(row, "set_id")
                target_set_id = runtime.mappings.beatmapsets.get(source_set_id)
                if target_set_id is None:
                    _dependency_missing(runtime, _PHASE_MAPS, "map", source_map_id, "mapset", source_set_id)
                    continue
                try:
                    source_code = _source_code(row["server"])
                    file_metadata = read_beatmap_file(runtime.config.data_directory, source_map_id, row["md5"])
                    stored = await upload_beatmap_file(
                        object_storage,
                        file_metadata,
                        source_code=source_code,
                        beatmapset_id=source_set_id,
                        beatmap_id=source_map_id,
                        invocation_id=runtime.report.invocation_id,
                        migration_id=runtime.config.migration_id,
                    )
                    staged.append(_stage_map(runtime, row, target_set_id, source_code, file_metadata, stored))
                except ObjectUploadFailed:
                    runtime.report.restore(snapshot)
                    raise
                except (MigrationStorageError, KeyError, TypeError, ValueError) as error:
                    _diagnose(runtime, _PHASE_MAPS, "map_file_invalid", error, "map", source_map_id)
                    runtime.report.increment(_PHASE_MAPS, "skipped_file")

            mapped: list[tuple[int, int, str, int]] = []
            try:
                async with runtime.session_factory.begin() as session:
                    for item in staged:
                        try:
                            async with session.begin_nested():
                                beatmap_id, revision_id, outcome = await _persist_map(runtime, session, item)
                        except IntegrityError as error:
                            _diagnose(runtime, _PHASE_MAPS, "map_target_conflict", error, "map", item.source_id)
                            continue
                        mapped.append((item.source_id, beatmap_id, item.file.md5.hex(), revision_id))
                        runtime.report.increment(_PHASE_MAPS, outcome)
                    cursor = int(rows[-1]["id"])
                    checkpoint = next_checkpoint(checkpoint, phase=_PHASE_MAPS, cursor=cursor)
                    await runtime.state.save(session, checkpoint)
            except BaseException:
                runtime.report.restore(snapshot)
                raise
            for source_map_id, beatmap_id, md5, revision_id in mapped:
                runtime.mappings.beatmaps[source_map_id] = beatmap_id
                runtime.mappings.revisions_by_md5[md5] = revision_id
            observer.batch_committed(len(rows))
            runtime.report.write(runtime.config.report_path)
        await complete_phase(runtime, checkpoint, _PHASE_MAPS)


async def _migrate_favourites(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            try:
                account_source_id = _positive(row, "userid")
                set_source_id = _positive(row, "setid")
                account_id = runtime.mappings.accounts.get(account_source_id)
                set_id = runtime.mappings.beatmapsets.get(set_source_id)
                if account_id is None:
                    _dependency_missing(
                        runtime,
                        _PHASE_FAVOURITES,
                        "favourite",
                        f"{account_source_id}:{set_source_id}",
                        "account",
                        account_source_id,
                    )
                    continue
                if set_id is None:
                    _dependency_missing(
                        runtime,
                        _PHASE_FAVOURITES,
                        "favourite",
                        f"{account_source_id}:{set_source_id}",
                        "mapset",
                        set_source_id,
                    )
                    continue
                created_at = unix_datetime(row["created_at"], fallback=runtime.started_at)
                statement = insert(BeatmapsetFavourite).values(
                    account_id=account_id,
                    beatmapset_id=set_id,
                    created_at=created_at,
                )
                await session.execute(statement.on_conflict_do_nothing())
                runtime.report.increment(_PHASE_FAVOURITES, "merged")
            except (KeyError, TypeError, ValueError) as error:
                _diagnose(
                    runtime,
                    _PHASE_FAVOURITES,
                    "favourite_invalid",
                    error,
                    "favourite",
                    f"{row.get('userid')}:{row.get('setid')}",
                )

    await _run_rows_by_account(
        runtime,
        phase=_PHASE_FAVOURITES,
        table="favourites",
        order_by=("userid", "setid"),
        handler=handler,
    )


async def _migrate_ratings(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            try:
                account_source_id = _positive(row, "userid")
                md5 = _md5_hex(row["map_md5"])
                account_id = runtime.mappings.accounts.get(account_source_id)
                revision_id = runtime.mappings.revisions_by_md5.get(md5)
                source_id = f"{account_source_id}:{md5}"
                if account_id is None:
                    _dependency_missing(runtime, _PHASE_RATINGS, "rating", source_id, "account", account_source_id)
                    continue
                if revision_id is None:
                    _dependency_missing(runtime, _PHASE_RATINGS, "rating", source_id, "beatmap_revision", md5)
                    continue
                beatmap_id = await session.scalar(
                    select(BeatmapRevision.beatmap_id).where(BeatmapRevision.id == revision_id)
                )
                if beatmap_id is None:
                    _dependency_missing(runtime, _PHASE_RATINGS, "rating", source_id, "beatmap", md5)
                    continue
                rating = bounded_integer(row["rating"], "rating", minimum=1, maximum=10)
                statement = insert(RatingVote).values(
                    account_id=account_id,
                    beatmap_id=beatmap_id,
                    rating=rating,
                )
                await session.execute(statement.on_conflict_do_nothing())
                runtime.report.increment(_PHASE_RATINGS, "merged")
            except (KeyError, TypeError, ValueError) as error:
                _diagnose(
                    runtime,
                    _PHASE_RATINGS,
                    "rating_invalid",
                    error,
                    "rating",
                    f"{row.get('userid')}:{row.get('map_md5')}",
                )

    await _run_rows_by_account(
        runtime,
        phase=_PHASE_RATINGS,
        table="ratings",
        order_by=("userid", "map_md5"),
        handler=handler,
    )


async def _migrate_map_requests(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            try:
                source_id = _positive(row, "id")
                source_map_id = _positive(row, "map_id")
                source_account_id = _positive(row, "player_id")
                beatmap_id = runtime.mappings.beatmaps.get(source_map_id)
                account_id = runtime.mappings.accounts.get(source_account_id)
                if beatmap_id is None:
                    _dependency_missing(runtime, _PHASE_MAP_REQUESTS, "map_request", source_id, "map", source_map_id)
                    continue
                if account_id is None:
                    _dependency_missing(
                        runtime, _PHASE_MAP_REQUESTS, "map_request", source_id, "account", source_account_id
                    )
                    continue
                active = _boolean(row["active"], "map request active")
                created_at = aware_datetime(
                    row["datetime"], runtime.config.source_timezone, fallback=runtime.started_at
                )
                statement = insert(MapStatusRequest).values(
                    id=runtime.ids.make("map-status-request", source_id),
                    beatmap_id=beatmap_id,
                    requester_account_id=account_id,
                    requested_status=BeatmapStatus.RANKED,
                    status="open" if active else "closed",
                    reason="Migrated from bancho.py map_requests.",
                    created_at=created_at,
                )
                await session.execute(statement.on_conflict_do_nothing())
                runtime.report.increment(_PHASE_MAP_REQUESTS, "merged")
            except (KeyError, TypeError, ValueError) as error:
                _diagnose(
                    runtime,
                    _PHASE_MAP_REQUESTS,
                    "map_request_invalid",
                    error,
                    "map_request",
                    row.get("id"),
                )

    await run_batched_phase(
        runtime,
        phase=_PHASE_MAP_REQUESTS,
        table="map_requests",
        key="id",
        handler=handler,
    )


async def _migrate_comments(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            try:
                source_id = _positive(row, "id")
                target_type = _text(row["target_type"], "comment target type", maximum=16).casefold()
                if target_type == "replay":
                    runtime.report.increment(_PHASE_COMMENTS, "deferred_to_scoring")
                    continue
                source_account_id = _positive(row, "userid")
                account_id = runtime.mappings.accounts.get(source_account_id)
                if account_id is None:
                    _dependency_missing(runtime, _PHASE_COMMENTS, "comment", source_id, "account", source_account_id)
                    continue
                source_target_id = _positive(row, "target_id")
                values: dict[str, object] = {
                    "author_account_id": account_id,
                    "position_ms": _position_ms(row["time"]),
                    "body": _text(row["comment"], "comment body", maximum=1000, strip=False),
                    "color": _color(row["colour"]),
                    "moderation_state": "visible",
                }
                if target_type == "map":
                    target_id = runtime.mappings.beatmaps.get(source_target_id)
                    dependency = "map"
                    values["beatmap_id"] = target_id
                elif target_type == "song":
                    target_id = runtime.mappings.beatmapsets.get(source_target_id)
                    dependency = "mapset"
                    values["beatmapset_id"] = target_id
                else:
                    raise ValueError(f"unsupported comment target type {target_type!r}")
                if target_id is None:
                    _dependency_missing(runtime, _PHASE_COMMENTS, "comment", source_id, dependency, source_target_id)
                    continue
                await session.execute(insert(Comment).values(**values))
                runtime.report.increment(_PHASE_COMMENTS, "inserted")
            except (KeyError, TypeError, ValueError) as error:
                _diagnose(
                    runtime,
                    _PHASE_COMMENTS,
                    "comment_invalid",
                    error,
                    "comment",
                    row.get("id"),
                )

    await run_batched_phase(
        runtime,
        phase=_PHASE_COMMENTS,
        table="comments",
        key="id",
        handler=handler,
    )


def _derive_set_metadata(runtime: MigrationRuntime, row: SourceRow, maps: list[SourceRow]) -> _SetMetadata:
    if not maps:
        raise ValueError("mapset has no source maps from which metadata can be derived")
    latest = maps[0]
    source_code = _source_code(row["server"])
    if any(_source_code(item["server"]) != source_code for item in maps):
        raise ValueError("mapset source does not match its maps")
    updates = tuple(
        aware_datetime(item["last_update"], runtime.config.source_timezone, fallback=runtime.started_at)
        for item in maps
    )
    last_checked = aware_datetime(
        row["last_osuapi_check"],
        runtime.config.source_timezone,
        fallback=max(updates),
    )
    return _SetMetadata(
        source_code=source_code,
        external_id=_positive(row, "id"),
        creator_name=_text(latest["creator"], "mapset creator", maximum=64),
        artist=_text(latest["artist"], "mapset artist", maximum=255),
        title=_text(latest["title"], "mapset title", maximum=255),
        status=beatmap_status(latest["status"]),
        last_source_update_at=max(updates),
        last_checked_at=last_checked,
    )


async def _persist_mapset(session: AsyncSession, metadata: _SetMetadata) -> tuple[int, bool]:
    source_id = await session.scalar(select(ContentSource.id).where(ContentSource.code == metadata.source_code))
    if source_id is None:
        raise RuntimeError(f"content source {metadata.source_code!r} is not available")
    statement = (
        insert(Beatmapset)
        .values(
            source_id=source_id,
            external_id=metadata.external_id,
            creator_name=metadata.creator_name,
            artist=metadata.artist,
            title=metadata.title,
            tags="",
            status=metadata.status,
            last_source_update_at=metadata.last_source_update_at,
            available=True,
            nsfw=False,
        )
        .on_conflict_do_nothing(index_elements=(Beatmapset.source_id, Beatmapset.external_id))
        .returning(Beatmapset.id)
    )
    beatmapset_id = await session.scalar(statement)
    created = beatmapset_id is not None
    if beatmapset_id is None:
        beatmapset_id = await session.scalar(
            select(Beatmapset.id).where(
                Beatmapset.source_id == source_id,
                Beatmapset.external_id == metadata.external_id,
            )
        )
    if beatmapset_id is None:
        raise RuntimeError("target-first mapset merge did not resolve an identity")
    sync_statement = insert(ContentSyncState).values(
        beatmapset_id=beatmapset_id,
        last_checked_at=metadata.last_checked_at,
        next_check_at=metadata.last_checked_at + timedelta(hours=24),
        unchanged_count=0,
        error_count=0,
    )
    await session.execute(sync_statement.on_conflict_do_nothing(index_elements=(ContentSyncState.beatmapset_id,)))
    return beatmapset_id, created


def _stage_map(
    runtime: MigrationRuntime,
    row: SourceRow,
    beatmapset_id: int,
    source_code: str,
    file: BeatmapFileMetadata,
    stored_object: StoredObject,
) -> _StagedMap:
    total_length = bounded_integer(row["total_length"], "total length", minimum=0, maximum=2_147_483)
    return _StagedMap(
        source_id=_positive(row, "id"),
        source_code=source_code,
        external_id=_positive(row, "id"),
        beatmapset_id=beatmapset_id,
        status=beatmap_status(row["status"]),
        ruleset=source_ruleset(row["mode"]),
        difficulty_name=_text(row["version"], "difficulty name", maximum=255),
        status_locked=_boolean(row["frozen"], "frozen"),
        file_name=_file_name(row["filename"]),
        source_updated_at=aware_datetime(
            row["last_update"], runtime.config.source_timezone, fallback=runtime.started_at
        ),
        total_length_ms=total_length * 1000,
        bpm=_bounded_decimal(row["bpm"], "bpm", minimum=Decimal(0), maximum=Decimal("9999999.999")),
        circle_size=_difficulty(row["cs"], "circle size"),
        overall_difficulty=_difficulty(row["od"], "overall difficulty"),
        approach_rate=_difficulty(row["ar"], "approach rate"),
        health_drain=_difficulty(row["hp"], "health drain"),
        max_combo=bounded_integer(row["max_combo"], "max combo", minimum=0, maximum=2_147_483_647),
        file=file,
        stored_object=stored_object,
    )


async def _persist_map(
    runtime: MigrationRuntime,
    session: AsyncSession,
    item: _StagedMap,
) -> tuple[int, int, str]:
    source_id = await session.scalar(select(ContentSource.id).where(ContentSource.code == item.source_code))
    if source_id is None:
        raise RuntimeError(f"content source {item.source_code!r} is not available")
    map_statement = (
        insert(Beatmap)
        .values(
            beatmapset_id=item.beatmapset_id,
            source_id=source_id,
            external_id=item.external_id,
            ruleset=item.ruleset,
            difficulty_name=item.difficulty_name,
            status=item.status,
            status_locked=item.status_locked,
        )
        .on_conflict_do_nothing(index_elements=(Beatmap.source_id, Beatmap.external_id))
        .returning(Beatmap.id)
    )
    beatmap_id = await session.scalar(map_statement)
    created_map = beatmap_id is not None
    if beatmap_id is None:
        beatmap_id = await session.scalar(
            select(Beatmap.id).where(Beatmap.source_id == source_id, Beatmap.external_id == item.external_id)
        )
    if beatmap_id is None:
        raise RuntimeError("target-first map merge did not resolve an identity")
    if created_map:
        session.add(
            BeatmapStatusEvent(
                beatmap_id=beatmap_id,
                previous_status=None,
                status=item.status,
                source="migration",
                reason=f"Imported by migration {runtime.config.migration_id}"[:255],
                effective_at=item.source_updated_at,
            )
        )

    asset_id = await _ensure_media_asset(runtime, session, item.stored_object)
    existing = (
        await session.execute(
            select(
                BeatmapRevision.id,
                BeatmapRevision.beatmap_id,
                BeatmapRevision.file_asset_id,
                BeatmapRevision.sha256,
            ).where(BeatmapRevision.md5 == item.file.md5)
        )
    ).one_or_none()
    if existing is not None:
        if existing.beatmap_id != beatmap_id or existing.sha256 != item.file.sha256:
            raise ValueError("existing beatmap revision MD5 belongs to different immutable content")
        if existing.file_asset_id is None:
            await session.execute(
                update(BeatmapRevision)
                .where(BeatmapRevision.id == existing.id, BeatmapRevision.file_asset_id.is_(None))
                .values(file_asset_id=asset_id)
            )
        return beatmap_id, existing.id, "exact_md5_reused"

    current_revision_id = await session.scalar(
        select(BeatmapRevision.id)
        .where(BeatmapRevision.beatmap_id == beatmap_id, BeatmapRevision.is_current.is_(True))
        .with_for_update()
    )
    revision_statement = (
        insert(BeatmapRevision)
        .values(
            beatmap_id=beatmap_id,
            file_asset_id=asset_id,
            md5=item.file.md5,
            sha256=item.file.sha256,
            file_name=item.file_name,
            file_name_key=func.lower(item.file_name),
            source_updated_at=item.source_updated_at,
            total_length_ms=item.total_length_ms,
            drain_length_ms=item.file.drain_length_ms,
            bpm=item.bpm,
            circle_size=item.circle_size,
            overall_difficulty=item.overall_difficulty,
            approach_rate=item.approach_rate,
            health_drain=item.health_drain,
            object_count=item.file.object_count,
            circle_count=item.file.circle_count,
            slider_count=item.file.slider_count,
            spinner_count=item.file.spinner_count,
            max_combo=item.max_combo,
            has_storyboard=item.file.has_storyboard,
            has_video=item.file.has_video,
            is_current=current_revision_id is None,
        )
        .on_conflict_do_nothing()
        .returning(BeatmapRevision.id)
    )
    revision_id = await session.scalar(revision_statement)
    if revision_id is None:
        revision_id = await session.scalar(select(BeatmapRevision.id).where(BeatmapRevision.md5 == item.file.md5))
    if revision_id is None:
        raise RuntimeError("immutable revision merge did not resolve by MD5")
    return (
        beatmap_id,
        revision_id,
        "revision_inserted" if current_revision_id is None else "historical_revision_inserted",
    )


async def _ensure_media_asset(
    runtime: MigrationRuntime,
    session: AsyncSession,
    stored_object: StoredObject,
) -> uuid.UUID:
    digest = stored_object.sha256
    if digest is None:
        raise ValueError("uploaded beatmap object has no SHA-256 digest")
    asset_statement = (
        insert(MediaAsset)
        .values(
            id=runtime.ids.make("beatmap-file", digest.hex()),
            storage_key=stored_object.storage_key,
            sha256=digest,
            media_type=stored_object.media_type,
            size_bytes=stored_object.size_bytes,
        )
        .on_conflict_do_nothing()
        .returning(MediaAsset.id)
    )
    asset_id = await session.scalar(asset_statement)
    if asset_id is None:
        asset_id = await session.scalar(select(MediaAsset.id).where(MediaAsset.sha256 == digest))
    if asset_id is None:
        raise RuntimeError("media asset merge did not resolve by SHA-256")
    return asset_id


async def _populate_content_mappings(runtime: MigrationRuntime) -> None:
    async with runtime.session_factory() as session:
        set_rows = (
            await session.execute(
                select(Beatmapset.external_id, Beatmapset.id)
                .join(ContentSource, ContentSource.id == Beatmapset.source_id)
                .where(ContentSource.code.in_(("osu", "private")))
                .order_by(ContentSource.official.desc(), Beatmapset.id)
            )
        ).all()
        map_rows = (
            await session.execute(
                select(Beatmap.external_id, Beatmap.id)
                .join(ContentSource, ContentSource.id == Beatmap.source_id)
                .where(ContentSource.code.in_(("osu", "private")))
                .order_by(ContentSource.official.desc(), Beatmap.id)
            )
        ).all()
        revision_rows = (await session.execute(select(BeatmapRevision.md5, BeatmapRevision.id))).all()
    for external_id, target_id in set_rows:
        runtime.mappings.beatmapsets.setdefault(external_id, target_id)
    for external_id, target_id in map_rows:
        runtime.mappings.beatmaps.setdefault(external_id, target_id)
    for md5, revision_id in revision_rows:
        runtime.mappings.revisions_by_md5.setdefault(md5.hex(), revision_id)


async def _run_rows_by_account(
    runtime: MigrationRuntime,
    *,
    phase: str,
    table: str,
    order_by: Sequence[str],
    handler: _RowsHandler,
) -> None:
    with PhaseObserver(runtime, phase) as observer:
        checkpoint = await _checkpoint(runtime)
        if phase in checkpoint.completed_phases:
            runtime.report.increment(phase, "resumed_complete", 0)
            observer.skipped()
            return
        cursor = checkpoint.cursor if checkpoint.phase == phase else 0
        for accounts in runtime.source.iter_batches(
            "users",
            key="id",
            batch_size=runtime.config.batch_size,
            start_after=cursor,
            columns=("id",),
        ):
            source_ids = [_positive(row, "id") for row in accounts]
            placeholders = ", ".join("%s" for _ in source_ids)
            rows = runtime.source.fetch_all(
                table,
                where=f"`userid` IN ({placeholders})",
                parameters=source_ids,
                order_by=order_by,
            )
            snapshot = runtime.report.snapshot()
            try:
                async with runtime.session_factory.begin() as session:
                    await handler(session, rows)
                    cursor = source_ids[-1]
                    checkpoint = next_checkpoint(checkpoint, phase=phase, cursor=cursor)
                    await runtime.state.save(session, checkpoint)
            except BaseException:
                runtime.report.restore(snapshot)
                raise
            observer.batch_committed(len(rows))
            runtime.report.write(runtime.config.report_path)
        await complete_phase(runtime, checkpoint, phase)


async def _checkpoint(runtime: MigrationRuntime) -> MigrationCheckpoint:
    checkpoint = await runtime.state.load()
    if checkpoint is None:
        raise RuntimeError("migration state is not initialized")
    return checkpoint


def _source_server(value: object) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="strict")
    candidate = str(value).strip().casefold()
    if candidate in {"osu", "osu!"}:
        return "osu!"
    if candidate == "private":
        return "private"
    raise ValueError(f"unsupported content server: {value!r}")


def _source_code(value: object) -> str:
    return "osu" if _source_server(value) == "osu!" else "private"


def _positive(row: SourceRow, key: str) -> int:
    return bounded_integer(row[key], key, minimum=1, maximum=9_223_372_036_854_775_807)


def _boolean(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    raise ValueError(f"{name} must be boolean")


def _text(value: object, name: str, *, maximum: int, strip: bool = True) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise ValueError(f"{name} must be text")
    result = value.strip() if strip else value
    if not result or len(result) > maximum or "\0" in result:
        raise ValueError(f"{name} must contain between 1 and {maximum} characters")
    return result


def _file_name(value: object) -> str:
    result = _text(value, "beatmap filename", maximum=255)
    if "/" in result or "\\" in result:
        raise ValueError("beatmap filename must be a basename")
    return result


def _bounded_decimal(
    value: object,
    name: str,
    *,
    minimum: Decimal,
    maximum: Decimal,
) -> Decimal:
    result = decimal_value(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _difficulty(value: object, name: str) -> Decimal:
    return _bounded_decimal(value, name, minimum=Decimal(0), maximum=Decimal(20))


def _md5_hex(value: object) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii")
    if not isinstance(value, str) or len(value) != 32:
        raise ValueError("beatmap MD5 must contain 32 hexadecimal characters")
    try:
        digest = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("beatmap MD5 must be hexadecimal") from error
    if len(digest) != 16:
        raise ValueError("beatmap MD5 must contain 16 bytes")
    return value.lower()


def _position_ms(value: object) -> int:
    position = decimal_value(value, "comment position") * 1000
    if position < 0 or position > 2_147_483_647:
        raise ValueError("comment position is outside the supported range")
    return int(position)


def _color(value: object) -> str | None:
    if value is None or value == "":
        return None
    color = _text(value, "comment color", maximum=7).removeprefix("#")
    if _COLOR.fullmatch(color) is None:
        raise ValueError("comment color must contain six hexadecimal characters")
    return f"#{color.upper()}"


def _dependency_missing(
    runtime: MigrationRuntime,
    phase: str,
    entity: str,
    source_id: object,
    dependency: str,
    dependency_id: object,
) -> None:
    runtime.report.add(
        DiagnosticSeverity.WARNING,
        "content_dependency_missing",
        f"{entity} was skipped because its migrated {dependency} is unavailable",
        entity=entity,
        source_id=source_id,
        details={"dependency": dependency, "dependency_source_id": str(dependency_id)},
    )
    runtime.report.increment(phase, "skipped_dependency")


def _diagnose(
    runtime: MigrationRuntime,
    phase: str,
    code: str,
    error: Exception,
    entity: str,
    source_id: object,
) -> None:
    runtime.report.add(
        DiagnosticSeverity.WARNING,
        code,
        str(error),
        entity=entity,
        source_id=source_id,
    )
    runtime.report.increment(phase, "skipped_invalid")
