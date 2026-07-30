"""Migrate legacy tournament pools into immutable multiplayer revisions."""

from __future__ import annotations

import hashlib
import json
import unicodedata
import uuid

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.multiplayer import TournamentPool, TournamentPoolItem, TournamentPoolRevision
from perfcho.infra.db.models.scoring import ModSet
from tools.bancho_migration.domains.common import run_batched_phase
from tools.bancho_migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.bancho_migration.transforms import aware_datetime, mod_set


async def migrate_multiplayer(runtime: MigrationRuntime) -> None:
    """Migrate tournament pool identities, one immutable revision, and all valid picks."""
    await _reconstruct_pool_mappings(runtime)

    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            await _migrate_pool(session, runtime, row)

    await run_batched_phase(
        runtime,
        phase="multiplayer.tournament_pools",
        table="tourney_pools",
        key="id",
        handler=handler,
    )


async def _reconstruct_pool_mappings(runtime: MigrationRuntime) -> None:
    source_rows = runtime.source.fetch_all("tourney_pools", columns=("id", "name"), order_by=("id",))
    names = {_name_key(row.get("name")) for row in source_rows}
    async with runtime.session_factory() as session:
        target_rows = (
            await session.execute(
                select(TournamentPool.name_key, TournamentPool.id).where(
                    TournamentPool.namespace == "bancho",
                    TournamentPool.name_key.in_(names),
                )
            )
        ).all()
    by_name = dict(target_rows)
    for row in source_rows:
        source_id = _positive(row.get("id"), "pool id")
        target_id = by_name.get(_name_key(row.get("name")))
        if target_id is not None:
            runtime.mappings.tournament_pools[source_id] = target_id


async def _migrate_pool(session: AsyncSession, runtime: MigrationRuntime, row: SourceRow) -> None:
    source_id = row.get("id")
    try:
        pool_id = _positive(source_id, "pool id")
        name = _name(row.get("name"))
        name_key = name.casefold()
        creator_id = runtime.mappings.accounts[_positive(row.get("created_by"), "pool creator")]
        created_at = aware_datetime(
            row.get("created_at"),
            runtime.config.source_timezone,
            fallback=runtime.started_at,
        )
        target_id = runtime.mappings.tournament_pools.get(pool_id)
        if target_id is None:
            target_id = runtime.ids.make("tournament-pool", pool_id)
            statement = (
                insert(TournamentPool)
                .values(
                    id=target_id,
                    namespace="bancho",
                    name=name,
                    name_key=name_key,
                    creator_account_id=creator_id,
                    status="published",
                    created_at=created_at,
                    updated_at=created_at,
                )
                .on_conflict_do_nothing(index_elements=(TournamentPool.namespace, TournamentPool.name_key))
                .returning(TournamentPool.id)
            )
            persisted = await session.scalar(statement)
            if persisted is None:
                persisted = await session.scalar(
                    select(TournamentPool.id).where(
                        TournamentPool.namespace == "bancho",
                        TournamentPool.name_key == name_key,
                    )
                )
            if persisted is None:
                raise RuntimeError("tournament pool merge did not resolve an identity")
            target_id = persisted
            runtime.mappings.tournament_pools[pool_id] = target_id

        picks = runtime.source.fetch_all(
            "tourney_pool_maps",
            where="`pool_id` = %s",
            parameters=(pool_id,),
            order_by=("mods", "slot", "map_id"),
        )
        configuration = [
            {"map_id": item.get("map_id"), "mods": item.get("mods"), "slot": item.get("slot")} for item in picks
        ]
        configuration_digest = hashlib.sha256(
            json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
        ).digest()
        actual_revision_id = await session.scalar(
            select(TournamentPoolRevision.id).where(
                TournamentPoolRevision.pool_id == target_id,
                TournamentPoolRevision.configuration_digest == configuration_digest,
            )
        )
        has_current = await session.scalar(
            select(TournamentPoolRevision.id).where(
                TournamentPoolRevision.pool_id == target_id,
                TournamentPoolRevision.is_current.is_(True),
            )
        )
        if actual_revision_id is None:
            latest_revision = await session.scalar(
                select(func.coalesce(func.max(TournamentPoolRevision.revision_number), 0)).where(
                    TournamentPoolRevision.pool_id == target_id
                )
            )
            revision_id = runtime.ids.make("tournament-pool-revision", pool_id)
            await session.execute(
                insert(TournamentPoolRevision)
                .values(
                    id=revision_id,
                    pool_id=target_id,
                    revision_number=int(latest_revision or 0) + 1,
                    created_by_id=creator_id,
                    configuration_digest=configuration_digest,
                    state="published",
                    is_current=has_current is None,
                    created_at=created_at,
                )
                .on_conflict_do_nothing()
            )
            actual_revision_id = await session.scalar(
                select(TournamentPoolRevision.id).where(
                    TournamentPoolRevision.pool_id == target_id,
                    TournamentPoolRevision.configuration_digest == configuration_digest,
                )
            )
        if actual_revision_id is None:
            raise RuntimeError("tournament pool revision conflicts with target data")
        for pick in picks:
            try:
                await _migrate_pick(session, runtime, pool_id, actual_revision_id, pick)
            except (KeyError, TypeError, ValueError) as error:
                runtime.report.add(
                    DiagnosticSeverity.WARNING,
                    "tournament_pool_item_skipped",
                    str(error),
                    entity="tourney_pool_maps",
                    source_id=f"{pool_id}:{pick.get('map_id')}",
                )
                runtime.report.increment("multiplayer.tournament_pools", "item_skipped")
        runtime.report.increment("multiplayer.tournament_pools", "merged")
    except (KeyError, TypeError, ValueError) as error:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "tournament_pool_skipped",
            str(error),
            entity="tourney_pools",
            source_id=source_id,
        )
        runtime.report.increment("multiplayer.tournament_pools", "skipped")


async def _migrate_pick(
    session: AsyncSession,
    runtime: MigrationRuntime,
    source_pool_id: int,
    revision_id: uuid.UUID,
    row: SourceRow,
) -> None:
    source_map_id = _positive(row.get("map_id"), "pool map id")
    beatmap_id = runtime.mappings.beatmaps.get(source_map_id)
    if beatmap_id is None:
        raise ValueError(f"pool map {source_map_id} was not migrated")
    map_row = runtime.source.fetch_all(
        "maps",
        columns=("md5", "mode"),
        where="`id` = %s",
        parameters=(source_map_id,),
    )
    if not map_row:
        raise ValueError(f"source map {source_map_id} is missing")
    source_map = map_row[0]
    md5 = _md5(source_map.get("md5"))
    revision_target_id = runtime.mappings.revisions_by_md5.get(md5)
    if revision_target_id is None:
        raise ValueError(f"pool map revision {md5} was not migrated")
    scoreboard_id, canonical, digest, bits = mod_set(source_map.get("mode"), row.get("mods"))
    mod_set_id = await session.scalar(
        insert(ModSet)
        .values(
            scoreboard_id=scoreboard_id,
            canonical=canonical,
            canonical_digest=digest,
            legacy_bits=bits,
        )
        .on_conflict_do_nothing(index_elements=(ModSet.scoreboard_id, ModSet.canonical_digest))
        .returning(ModSet.id)
    )
    if mod_set_id is None:
        mod_set_id = await session.scalar(
            select(ModSet.id).where(ModSet.scoreboard_id == scoreboard_id, ModSet.canonical_digest == digest)
        )
    if mod_set_id is None:
        raise RuntimeError("pool mod set merge failed")
    slot = _positive(row.get("slot"), "pool slot")
    bucket = _mod_bucket(canonical)
    await session.execute(
        insert(TournamentPoolItem)
        .values(
            id=runtime.ids.make("tournament-pool-item", f"{source_pool_id}:{source_map_id}:{bits}:{slot}"),
            revision_id=revision_id,
            beatmap_revision_id=revision_target_id,
            scoreboard_id=scoreboard_id,
            mod_set_id=mod_set_id,
            mod_bucket=bucket,
            slot_number=slot,
        )
        .on_conflict_do_nothing()
    )


def _name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("pool name must be text")
    result = unicodedata.normalize("NFKC", value).strip()
    if not 1 <= len(result) <= 100:
        raise ValueError("pool name length is invalid")
    return result


def _name_key(value: object) -> str:
    return _name(value).casefold()


def _positive(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _md5(value: object) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii")
    if not isinstance(value, str) or len(value) != 32:
        raise ValueError("map MD5 must contain 32 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("map MD5 must be hexadecimal") from error
    return value.lower()


def _mod_bucket(canonical: list[dict[str, object]]) -> str:
    acronyms = [str(item.get("acronym", "")) for item in canonical]
    result = "".join(acronyms) or "NM"
    return result[:16]
