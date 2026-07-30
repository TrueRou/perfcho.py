"""Migrate bancho.py relationships and clans into canonical social facts."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import Ruleset, TeamRole
from perfcho.infra.db.models.social import (
    AchievementDefinition,
    AchievementTranslation,
    AchievementUnlock,
    Block,
    Follow,
    Team,
    TeamMembership,
)
from tools.bancho_migration.domains.common import run_batched_phase, run_single_phase
from tools.bancho_migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.bancho_migration.transforms import aware_datetime, unix_datetime


@dataclass(frozen=True, slots=True)
class _PreparedTeam:
    source_id: int
    name: str
    name_key: str
    tag: str
    tag_key: str
    created_at: datetime


async def migrate_social(runtime: MigrationRuntime) -> None:
    """Migrate follows, blocks, teams, and current team memberships."""
    await _reconstruct_team_mappings(runtime)

    async def teams_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_team_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="social.teams",
        table="clans",
        key="id",
        handler=teams_handler,
    )

    async def sequence_handler(session: AsyncSession) -> None:
        await session.execute(
            text(
                """
                SELECT setval(
                    pg_get_serial_sequence('social.teams', 'id'),
                    GREATEST(1, (SELECT COALESCE(MAX(id), 0) FROM social.teams)),
                    true
                )
                """
            )
        )

    await run_single_phase(runtime, phase="social.team_sequence", handler=sequence_handler)

    async def memberships_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_membership_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="social.team_memberships",
        table="users",
        key="id",
        columns=("id", "clan_id", "clan_priv", "creation_time"),
        handler=memberships_handler,
    )

    async def relationships_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_relationship_batch(session, runtime, rows)

    # relationships has a composite key, so users.id is the lossless batch cursor.
    await run_batched_phase(
        runtime,
        phase="social.relationships",
        table="users",
        key="id",
        columns=("id",),
        handler=relationships_handler,
    )

    async def achievements_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_achievement_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="social.achievements",
        table="achievements",
        key="id",
        handler=achievements_handler,
    )

    async def unlocks_handler(session: AsyncSession, users: list[SourceRow]) -> None:
        await _migrate_unlock_batch(session, runtime, users)

    await run_batched_phase(
        runtime,
        phase="social.achievement_unlocks",
        table="users",
        key="id",
        columns=("id",),
        handler=unlocks_handler,
    )


async def _reconstruct_team_mappings(runtime: MigrationRuntime) -> None:
    for rows in runtime.source.iter_batches(
        "clans",
        key="id",
        batch_size=runtime.config.batch_size,
        columns=("id", "name", "tag", "created_at"),
    ):
        prepared = [_prepare_team(runtime, row, diagnose=False) for row in rows]
        valid = [team for team in prepared if team is not None]
        async with runtime.session_factory() as session:
            await _resolve_teams(session, runtime, valid, allow_create=False, diagnose=False)


async def _migrate_team_batch(session: AsyncSession, runtime: MigrationRuntime, rows: list[SourceRow]) -> None:
    prepared = [_prepare_team(runtime, row, diagnose=True) for row in rows]
    await _resolve_teams(
        session,
        runtime,
        [team for team in prepared if team is not None],
        allow_create=True,
        diagnose=True,
    )


def _prepare_team(runtime: MigrationRuntime, row: SourceRow, *, diagnose: bool) -> _PreparedTeam | None:
    try:
        source_id = _positive_int(row.get("id"), "clan id")
        raw_name = row.get("name")
        raw_tag = row.get("tag")
        if not isinstance(raw_name, str) or not isinstance(raw_tag, str):
            raise ValueError("clan name and tag must be text")
        name = unicodedata.normalize("NFKC", raw_name).strip()
        tag = unicodedata.normalize("NFKC", raw_tag).strip()
        if not 1 <= len(name) <= 100:
            raise ValueError("clan name must contain between 1 and 100 characters")
        if not 1 <= len(tag) <= 8:
            raise ValueError("clan tag must contain between 1 and 8 characters")
        return _PreparedTeam(
            source_id,
            name,
            name.casefold(),
            tag,
            tag.casefold(),
            _source_timestamp(runtime, row.get("created_at")),
        )
    except ValueError as error:
        if diagnose:
            runtime.report.increment("social.teams", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "team_malformed",
                str(error),
                entity="clans",
                source_id=row.get("id"),
            )
        return None


async def _resolve_teams(
    session: AsyncSession,
    runtime: MigrationRuntime,
    teams: list[_PreparedTeam],
    *,
    allow_create: bool,
    diagnose: bool,
) -> None:
    if not teams:
        return
    source_ids = {team.source_id for team in teams}
    name_keys = {team.name_key for team in teams}
    tag_keys = {team.tag_key for team in teams}
    existing_ids = set((await session.scalars(select(Team.id).where(Team.id.in_(source_ids)))).all())
    by_name = dict((await session.execute(select(Team.name_key, Team.id).where(Team.name_key.in_(name_keys)))).all())
    by_tag = dict((await session.execute(select(Team.tag_key, Team.id).where(Team.tag_key.in_(tag_keys)))).all())
    reverse = {target_id: source_id for source_id, target_id in runtime.mappings.teams.items()}
    pending: list[_PreparedTeam] = []
    for team in teams:
        candidates = {
            candidate
            for candidate in (
                team.source_id if team.source_id in existing_ids else None,
                by_name.get(team.name_key),
                by_tag.get(team.tag_key),
            )
            if candidate is not None
        }
        if len(candidates) > 1:
            if diagnose:
                _team_error(
                    runtime,
                    "team_resolution_ambiguous",
                    "clan ID, name, and tag resolve to different target teams",
                    team.source_id,
                    {"target_team_ids": sorted(candidates)},
                )
            continue
        target_id = next(iter(candidates), None)
        if target_id is None:
            if allow_create:
                pending.append(team)
            continue
        previous = reverse.get(target_id)
        if previous is not None and previous != team.source_id:
            if diagnose:
                _team_error(
                    runtime,
                    "team_resolution_ambiguous",
                    "multiple source clans resolve to one target team",
                    team.source_id,
                    {"target_team_id": target_id, "other_source_id": previous},
                )
            continue
        runtime.mappings.teams[team.source_id] = target_id
        reverse[target_id] = team.source_id
        runtime.report.increment("social.teams", "resolved")

    if pending:
        await session.execute(
            insert(Team)
            .values(
                [
                    {
                        "id": team.source_id,
                        "name": team.name,
                        "name_key": team.name_key,
                        "tag": team.tag,
                        "tag_key": team.tag_key,
                        "ruleset": Ruleset.OSU,
                        "created_at": team.created_at,
                        "updated_at": team.created_at,
                    }
                    for team in pending
                ]
            )
            .on_conflict_do_nothing()
        )
        persisted_ids = set(
            (await session.scalars(select(Team.id).where(Team.id.in_({team.source_id for team in pending})))).all()
        )
        for team in pending:
            if team.source_id not in persisted_ids:
                if diagnose:
                    _team_error(
                        runtime,
                        "team_insert_conflict",
                        "target data prevented insertion of the source clan",
                        team.source_id,
                    )
                continue
            runtime.mappings.teams[team.source_id] = team.source_id
            runtime.report.increment("social.teams", "imported")


async def _migrate_membership_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    role_by_value = {1: TeamRole.MEMBER, 2: TeamRole.OFFICER, 3: TeamRole.OWNER}
    values: list[dict[str, object]] = []
    for row in rows:
        try:
            source_account_id = _positive_int(row.get("id"), "user id")
            raw_team_id = row.get("clan_id")
            if raw_team_id in {None, 0}:
                continue
            source_team_id = _positive_int(raw_team_id, "clan id")
            account_id = runtime.mappings.accounts[source_account_id]
            team_id = runtime.mappings.teams[source_team_id]
            role_value = row.get("clan_priv")
            if isinstance(role_value, bool) or not isinstance(role_value, int) or role_value not in role_by_value:
                raise ValueError("clan privilege must be member, officer, or owner")
            values.append(
                {
                    "team_id": team_id,
                    "account_id": account_id,
                    "role": role_by_value[role_value],
                    "created_at": _source_timestamp(runtime, row.get("creation_time")),
                }
            )
        except (KeyError, ValueError) as error:
            runtime.report.increment("social.team_memberships", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "team_membership_malformed",
                str(error),
                entity="users",
                source_id=row.get("id"),
            )
    if values:
        # A target current membership or owner wins through the partial unique indexes.
        await session.execute(insert(TeamMembership).values(values).on_conflict_do_nothing())
    runtime.report.increment("social.team_memberships", "processed", len(values))


async def _migrate_relationship_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    users: list[SourceRow],
) -> None:
    source_ids = [_positive_int(row.get("id"), "user id") for row in users]
    placeholders = ", ".join(["%s"] * len(source_ids))
    rows = runtime.source.fetch_all(
        "relationships",
        columns=("user1", "user2", "type"),
        order_by=("user1", "user2"),
        where=f"`user1` IN ({placeholders})",
        parameters=source_ids,
    )
    candidates: list[tuple[int, int, str, object]] = []
    for row in rows:
        try:
            source_actor = _positive_int(row.get("user1"), "relationship actor")
            source_target = _positive_int(row.get("user2"), "relationship target")
            actor = runtime.mappings.accounts[source_actor]
            target = runtime.mappings.accounts[source_target]
            if actor == target:
                raise ValueError("self relationships are not supported")
            raw_kind = row.get("type")
            if not isinstance(raw_kind, str):
                raise ValueError("relationship type must be text")
            kind = raw_kind.strip().lower()
            if kind in {"friend", "follow"}:
                kind = "follow"
            elif kind != "block":
                raise ValueError(f"unsupported relationship type: {kind}")
            candidates.append((actor, target, kind, f"{source_actor}:{source_target}"))
        except (KeyError, ValueError) as error:
            runtime.report.increment("social.relationships", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "relationship_malformed",
                str(error),
                entity="relationships",
                source_id=f"{row.get('user1')}:{row.get('user2')}",
            )
    if not candidates:
        return

    pairs = {(actor, target) for actor, target, _, _ in candidates}
    reverse_pairs = {(target, actor) for actor, target in pairs}
    lookup_pairs = pairs | reverse_pairs
    target_follows: set[tuple[int, int]] = {
        (actor, target)
        for actor, target in (
            await session.execute(
                select(Follow.actor_account_id, Follow.target_account_id).where(
                    tuple_(Follow.actor_account_id, Follow.target_account_id).in_(lookup_pairs)
                )
            )
        ).all()
    }
    target_blocks = {
        (actor, target)
        for actor, target in (
            await session.execute(
                select(Block.actor_account_id, Block.target_account_id).where(
                    tuple_(Block.actor_account_id, Block.target_account_id).in_(lookup_pairs)
                )
            )
        ).all()
    }
    follows: list[dict[str, object]] = []
    blocks: list[dict[str, object]] = []
    imported_directions: set[tuple[int, int]] = set()
    for actor, target, kind, _ in candidates:
        if (actor, target) in target_blocks or (actor, target) in target_follows:
            runtime.report.increment("social.relationships", "target_wins")
            continue
        if (actor, target) in imported_directions:
            runtime.report.increment("social.relationships", "duplicate")
            continue
        value = {"actor_account_id": actor, "target_account_id": target, "created_at": runtime.started_at}
        (follows if kind == "follow" else blocks).append(value)
        imported_directions.add((actor, target))
    if follows:
        await session.execute(insert(Follow).values(follows).on_conflict_do_nothing())
    if blocks:
        await session.execute(insert(Block).values(blocks).on_conflict_do_nothing())
    runtime.report.increment("social.relationships", "imported", len(follows) + len(blocks))


async def _migrate_achievement_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    for row in rows:
        source_id = row.get("id")
        try:
            achievement_id = _positive_int(source_id, "achievement id")
            slug = _achievement_text(row.get("file"), "achievement file", 100)
            name = _achievement_text(row.get("name"), "achievement name", 100)
            description = _achievement_text(row.get("desc"), "achievement description", 1000)
            condition = _achievement_text(row.get("cond"), "achievement condition", 256)
            ruleset = _achievement_ruleset(slug)
            statement = (
                insert(AchievementDefinition)
                .values(
                    slug=slug,
                    evaluator_code="legacy_bancho_condition",
                    evaluator_version=1,
                    parameters={"condition": condition, "migration_id": runtime.config.migration_id},
                    ruleset=ruleset,
                    active=True,
                    created_at=runtime.started_at,
                    updated_at=runtime.started_at,
                )
                .on_conflict_do_nothing(index_elements=(AchievementDefinition.slug,))
                .returning(AchievementDefinition.id)
            )
            target_id = await session.scalar(statement)
            if target_id is None:
                target_id = await session.scalar(
                    select(AchievementDefinition.id).where(AchievementDefinition.slug == slug)
                )
            if target_id is None:
                raise RuntimeError("achievement merge did not resolve an identity")
            runtime.mappings.achievements[achievement_id] = target_id
            await session.execute(
                insert(AchievementTranslation)
                .values(
                    achievement_id=target_id,
                    locale="en",
                    name=name,
                    description=description,
                )
                .on_conflict_do_nothing()
            )
            runtime.report.increment("social.achievements", "merged")
        except (TypeError, ValueError) as error:
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "achievement_skipped",
                str(error),
                entity="achievements",
                source_id=source_id,
            )
            runtime.report.increment("social.achievements", "skipped")


async def _migrate_unlock_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    users: list[SourceRow],
) -> None:
    source_ids = [_positive_int(row.get("id"), "user id") for row in users]
    placeholders = ", ".join("%s" for _ in source_ids)
    rows = runtime.source.fetch_all(
        "user_achievements",
        where=f"`userid` IN ({placeholders})",
        parameters=source_ids,
        order_by=("userid", "achid"),
    )
    for row in rows:
        source_id = f"{row.get('userid')}:{row.get('achid')}"
        try:
            source_account_id = _positive_int(row.get("userid"), "unlock user id")
            source_achievement_id = _positive_int(row.get("achid"), "unlock achievement id")
            account_id = runtime.mappings.accounts[source_account_id]
            achievement_id = runtime.mappings.achievements.get(source_achievement_id)
            if achievement_id is None:
                achievement_id = await _achievement_target_id(runtime, session, source_achievement_id)
            if achievement_id is None:
                raise ValueError("achievement definition was not migrated")
            await session.execute(
                insert(AchievementUnlock)
                .values(
                    account_id=account_id,
                    achievement_id=achievement_id,
                    definition_version=1,
                    snapshot={"migration_id": runtime.config.migration_id},
                    created_at=runtime.started_at,
                )
                .on_conflict_do_nothing()
            )
            runtime.report.increment("social.achievement_unlocks", "merged")
        except (KeyError, TypeError, ValueError) as error:
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "achievement_unlock_skipped",
                str(error),
                entity="user_achievements",
                source_id=source_id,
            )
            runtime.report.increment("social.achievement_unlocks", "skipped")


async def _achievement_target_id(
    runtime: MigrationRuntime,
    session: AsyncSession,
    source_achievement_id: int,
) -> int | None:
    rows = runtime.source.fetch_all(
        "achievements",
        columns=("file",),
        where="`id` = %s",
        parameters=(source_achievement_id,),
    )
    if not rows:
        return None
    slug = _achievement_text(rows[0].get("file"), "achievement file", 100)
    target_id = await session.scalar(select(AchievementDefinition.id).where(AchievementDefinition.slug == slug))
    if target_id is not None:
        runtime.mappings.achievements[source_achievement_id] = target_id
    return target_id


def _achievement_ruleset(slug: str) -> Ruleset | None:
    prefix = slug.split("-", 1)[0]
    return {
        "osu": Ruleset.OSU,
        "taiko": Ruleset.TAIKO,
        "fruits": Ruleset.FRUITS,
        "mania": Ruleset.MANIA,
    }.get(prefix)


def _achievement_text(value: object, name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be text")
    result = value.strip()
    if not result or len(result) > maximum:
        raise ValueError(f"{name} length is invalid")
    return result


def _source_timestamp(runtime: MigrationRuntime, value: object) -> datetime:
    if isinstance(value, datetime):
        return aware_datetime(value, runtime.config.source_timezone, fallback=runtime.started_at)
    return unix_datetime(value, fallback=runtime.started_at)


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _team_error(
    runtime: MigrationRuntime,
    code: str,
    message: str,
    source_id: int,
    details: dict[str, object] | None = None,
) -> None:
    runtime.report.increment("social.teams", "skipped_ambiguous")
    runtime.report.add(
        DiagnosticSeverity.ERROR,
        code,
        message,
        entity="clans",
        source_id=source_id,
        details=details,
    )
