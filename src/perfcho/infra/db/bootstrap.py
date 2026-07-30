"""Seed the deterministic minimum PostgreSQL runtime catalog."""

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import FromClause

from perfcho.infra.db.advisory_lock import acquire_transaction_lock
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import AccountStatus, AccountType, ChannelKind, Ruleset, ScoreboardVariant
from perfcho.infra.db.models.authz import (
    AccountRoleGrant,
    Entitlement,
    Permission,
    Role,
    RolePermission,
)
from perfcho.infra.db.models.community import Channel
from perfcho.infra.db.models.content import ContentSource
from perfcho.infra.db.models.core import Account, AccountName, UserPreference, UserProfile
from perfcho.infra.db.models.iam import Scope
from perfcho.infra.db.models.scoring import ModPolicy, ModSet, RankingPolicy, Scoreboard
from perfcho.infra.db.models.system import ServerSetting

STABLE_PROTOCOL_VERSION = 19

_BOOTSTRAP_VERSION = 1
_BOOTSTRAP_EPOCH = datetime(2007, 9, 16, tzinfo=UTC)
_BOOTSTRAP_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://perfcho.dev/database-bootstrap")

OAUTH_SCOPES = (
    (1, "public", "Read public data."),
    (2, "identify", "Read the authenticated account's public identity."),
    (3, "friends.read", "Read the authenticated account's friends."),
    (4, "forum.write", "Create and edit forum content."),
    (5, "delegate", "Act with the authenticated account's delegated access."),
    (6, "chat.read", "Read chat channels and messages."),
    (7, "chat.write", "Send chat messages."),
    (8, "chat.write_manage", "Manage chat channels and messages."),
    (9, "lazer", "Access first-party osu!lazer operations."),
)

PERMISSIONS = (
    (1, "account.login", "Log in to the service."),
    (2, "chat.read", "Read visible chat channels and messages."),
    (3, "chat.write", "Send messages to writable chat channels."),
    (4, "chat.announce", "Publish server announcements."),
    (5, "chat.manage", "Manage channels and moderate messages."),
    (6, "content.read", "Read beatmap content and metadata."),
    (7, "content.submit", "Submit and update owned beatmap content."),
    (8, "content.manage", "Manage authoritative content state."),
    (9, "scoring.read", "Read scores, statistics, and leaderboards."),
    (10, "scoring.submit", "Submit gameplay attempts and scores."),
    (11, "scoring.manage", "Manage score validity and ranking projections."),
    (12, "multiplayer.play", "Join multiplayer rooms and rounds."),
    (13, "multiplayer.host", "Create and host multiplayer rooms."),
    (14, "multiplayer.manage", "Manage multiplayer rooms and tournaments."),
    (15, "moderation.read", "Read moderation cases and evidence."),
    (16, "moderation.enforce", "Apply and revoke moderation sanctions."),
    (17, "admin.access", "Perform security-sensitive administration."),
)

ROLES = (
    (1, "user", "User", "Default gameplay and community capabilities.", 1000, True),
    (2, "bot", "Bot", "Capabilities required by trusted runtime bots.", 500, True),
    (3, "moderator", "Moderator", "Community, content, and gameplay moderation.", 100, True),
    (4, "administrator", "Administrator", "Full service administration.", 0, True),
)

_USER_PERMISSION_CODES = {
    "account.login",
    "chat.read",
    "chat.write",
    "content.read",
    "content.submit",
    "scoring.read",
    "scoring.submit",
    "multiplayer.play",
    "multiplayer.host",
}
_BOT_PERMISSION_CODES = {
    "account.login",
    "chat.read",
    "chat.write",
    "chat.announce",
    "multiplayer.play",
}
_MODERATOR_PERMISSION_CODES = {code for _, code, _ in PERMISSIONS if code != "admin.access"}
_ADMIN_PERMISSION_CODES = {code for _, code, _ in PERMISSIONS}
ROLE_PERMISSION_CODES = {
    "user": _USER_PERMISSION_CODES,
    "bot": _BOT_PERMISSION_CODES,
    "moderator": _MODERATOR_PERMISSION_CODES,
    "administrator": _ADMIN_PERMISSION_CODES,
}

SCOREBOARDS = (
    (1, "osu", Ruleset.OSU, ScoreboardVariant.VANILLA),
    (2, "taiko", Ruleset.TAIKO, ScoreboardVariant.VANILLA),
    (3, "fruits", Ruleset.FRUITS, ScoreboardVariant.VANILLA),
    (4, "mania", Ruleset.MANIA, ScoreboardVariant.VANILLA),
    (5, "osu_relax", Ruleset.OSU, ScoreboardVariant.RELAX),
    (6, "taiko_relax", Ruleset.TAIKO, ScoreboardVariant.RELAX),
    (7, "fruits_relax", Ruleset.FRUITS, ScoreboardVariant.RELAX),
    (8, "osu_autopilot", Ruleset.OSU, ScoreboardVariant.AUTOPILOT),
)

STABLE_CODEC_LIMITS = {
    "max_body_size": 16 * 1024 * 1024,
    "max_packet_size": 2 * 1024 * 1024,
    "max_packet_count": 4096,
    "max_string_size": 64 * 1024,
    "max_list_length": 8192,
    "max_frame_count": 4096,
    "max_uleb128_bytes": 5,
}

_ACCOUNT_IDENTITY_SQL = text(
    """
    SELECT setval(
        pg_get_serial_sequence('core.accounts', 'id'),
        GREATEST(
            2,
            (SELECT COALESCE(MAX(id), 0) FROM core.accounts),
            COALESCE(
                (
                    SELECT last_value
                    FROM pg_sequences
                    WHERE schemaname = 'core'
                      AND sequencename = split_part(
                          pg_get_serial_sequence('core.accounts', 'id'),
                          '.',
                          2
                      )
                ),
                0
            )
        ),
        true
    )
    """
)


def canonical_json_digest(value: object) -> bytes:
    """Hash canonical compact JSON for persisted policy and mod-set identity."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).digest()


def _bootstrap_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(_BOOTSTRAP_NAMESPACE, name)


async def _upsert_catalog(
    session: AsyncSession,
    table: FromClause,
    rows: Sequence[Mapping[str, object]],
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] = (),
) -> None:
    statement = insert(cast(Any, table)).values(list(rows))
    index_elements = [table.c[name] for name in conflict_columns]
    if update_columns:
        statement = statement.on_conflict_do_update(
            index_elements=index_elements,
            set_={name: getattr(statement.excluded, name) for name in update_columns},
        )
    else:
        statement = statement.on_conflict_do_nothing(index_elements=index_elements)
    await session.execute(statement)


def _mod_policy_rules(scoreboard_code: str, variant: ScoreboardVariant) -> dict[str, object]:
    return {
        "scoreboard": scoreboard_code,
        "variant": variant.value,
        "allowed_acronyms": [
            "NF",
            "EZ",
            "TD",
            "HD",
            "HR",
            "SD",
            "DT",
            "HT",
            "NC",
            "FL",
            "SO",
            "PF",
            "FI",
            "MR",
        ],
        "required_acronyms": [],
        "forbidden_acronyms": ["AT", "CN", "RX", "AP"],
    }


async def _seed_identity(session: AsyncSession) -> None:
    await _upsert_catalog(
        session,
        Account.__table__,
        (
            {
                "id": 1,
                "type": AccountType.BOT,
                "status": AccountStatus.ACTIVE,
                "country_code": "SH",
                "registered_at": _BOOTSTRAP_EPOCH,
                "activated_at": _BOOTSTRAP_EPOCH,
                "auth_version": 1,
            },
        ),
        conflict_columns=("id",),
        update_columns=("type", "status", "country_code", "activated_at"),
    )
    await session.execute(_ACCOUNT_IDENTITY_SQL)

    name_statement = insert(AccountName).values(
        account_id=1,
        display_name="BanchoBot",
        name_key="banchobot",
        started_at=_BOOTSTRAP_EPOCH,
    )
    await session.execute(
        name_statement.on_conflict_do_update(
            index_elements=(AccountName.account_id,),
            index_where=AccountName.ended_at.is_(None),
            set_={
                "display_name": name_statement.excluded.display_name,
                "name_key": name_statement.excluded.name_key,
                "started_at": name_statement.excluded.started_at,
            },
        )
    )

    await _upsert_catalog(
        session,
        UserProfile.__table__,
        (
            {
                "account_id": 1,
                "bio": "The official server bot.",
                "location": "osu!",
                "website": "https://osu.ppy.sh/users/3",
                "social_links": {},
                "default_ruleset": Ruleset.OSU,
                "play_style": [],
            },
        ),
        conflict_columns=("account_id",),
        update_columns=("bio", "location", "website", "social_links", "default_ruleset", "play_style"),
    )
    await _upsert_catalog(
        session,
        UserPreference.__table__,
        (
            {
                "account_id": 1,
                "locale": "en",
                "timezone": "UTC",
                "theme": "system",
                "master_volume": 1.0,
                "music_volume": 1.0,
                "effect_volume": 1.0,
                "preferred_ranking_policy": "stable.osu.ranked",
                "private_message_policy": "friends",
                "invisible_online": False,
                "profile_section_order": [],
                "extra": {},
            },
        ),
        conflict_columns=("account_id",),
        update_columns=(
            "locale",
            "timezone",
            "theme",
            "master_volume",
            "music_volume",
            "effect_volume",
            "preferred_ranking_policy",
            "private_message_policy",
            "invisible_online",
            "profile_section_order",
            "extra",
        ),
    )


async def _seed_access_catalog(session: AsyncSession) -> None:
    await _upsert_catalog(
        session,
        Scope.__table__,
        tuple({"id": item_id, "code": code, "description": description} for item_id, code, description in OAUTH_SCOPES),
        conflict_columns=("id",),
        update_columns=("code", "description"),
    )
    await _upsert_catalog(
        session,
        Permission.__table__,
        tuple({"id": item_id, "code": code, "description": description} for item_id, code, description in PERMISSIONS),
        conflict_columns=("id",),
        update_columns=("code", "description"),
    )
    await _upsert_catalog(
        session,
        Role.__table__,
        tuple(
            {
                "id": item_id,
                "code": code,
                "name": name,
                "description": description,
                "priority": priority,
                "system": system,
            }
            for item_id, code, name, description, priority, system in ROLES
        ),
        conflict_columns=("id",),
        update_columns=("code", "name", "description", "priority", "system"),
    )

    permission_ids = {code: item_id for item_id, code, _ in PERMISSIONS}
    role_ids = {code: item_id for item_id, code, *_ in ROLES}
    await _upsert_catalog(
        session,
        RolePermission.__table__,
        tuple(
            {"role_id": role_ids[role_code], "permission_id": permission_ids[permission_code]}
            for role_code, permission_codes in ROLE_PERMISSION_CODES.items()
            for permission_code in sorted(permission_codes)
        ),
        conflict_columns=("role_id", "permission_id"),
    )
    await _upsert_catalog(
        session,
        AccountRoleGrant.__table__,
        (
            {
                "id": _bootstrap_uuid("account-role-grant:banchobot:bot"),
                "account_id": 1,
                "role_id": role_ids["bot"],
                "granted_by_id": None,
                "starts_at": _BOOTSTRAP_EPOCH,
                "reason": "Deterministic runtime bot grant.",
            },
        ),
        conflict_columns=("id",),
        update_columns=("account_id", "role_id", "starts_at", "reason"),
    )
    await _upsert_catalog(
        session,
        Entitlement.__table__,
        (
            {
                "id": 1,
                "code": "supporter",
                "name": "Supporter",
                "description": "Time-bounded supporter product benefits.",
            },
        ),
        conflict_columns=("id",),
        update_columns=("code", "name", "description"),
    )


async def _seed_scoring_catalog(session: AsyncSession) -> None:
    await _upsert_catalog(
        session,
        Scoreboard.__table__,
        tuple(
            {"id": item_id, "code": code, "ruleset": ruleset, "variant": variant, "active": True}
            for item_id, code, ruleset, variant in SCOREBOARDS
        ),
        conflict_columns=("id",),
        update_columns=("code", "ruleset", "variant", "active"),
    )

    no_mods: list[dict[str, object]] = []
    no_mod_digest = canonical_json_digest(no_mods)
    await _upsert_catalog(
        session,
        ModSet.__table__,
        tuple(
            {
                "scoreboard_id": scoreboard_id,
                "canonical": no_mods,
                "canonical_digest": no_mod_digest,
                "legacy_bits": 0,
            }
            for scoreboard_id, *_ in SCOREBOARDS
        ),
        conflict_columns=("scoreboard_id", "canonical_digest"),
        update_columns=("canonical", "legacy_bits"),
    )

    mod_policy_rows: list[dict[str, object]] = []
    ranking_policy_rows: list[dict[str, object]] = []
    for scoreboard_id, scoreboard_code, _, variant in SCOREBOARDS:
        rules = _mod_policy_rules(scoreboard_code, variant)
        mod_policy_id = _bootstrap_uuid(f"mod-policy:{scoreboard_code}:v{_BOOTSTRAP_VERSION}")
        mod_policy_rows.append(
            {
                "id": mod_policy_id,
                "name": f"Stable {scoreboard_code} ranked mods",
                "schema_version": _BOOTSTRAP_VERSION,
                "rules": rules,
                "digest": canonical_json_digest(rules),
            }
        )
        ranking_policy_rows.append(
            {
                "id": _bootstrap_uuid(f"ranking-policy:{scoreboard_code}:v{_BOOTSTRAP_VERSION}"),
                "code": f"stable.{scoreboard_code}.ranked",
                "version": _BOOTSTRAP_VERSION,
                "scoreboard_id": scoreboard_id,
                "metric": "total_score" if variant is ScoreboardVariant.VANILLA else "pp",
                "tie_breaker": "ended_at",
                "mod_policy_id": mod_policy_id,
                "configuration": {
                    "best_per_account": True,
                    "eligible_beatmap_statuses": (
                        ["ranked", "approved", "qualified", "loved"]
                        if variant is ScoreboardVariant.VANILLA
                        else ["ranked", "approved"]
                    ),
                    "performance_required": variant is not ScoreboardVariant.VANILLA,
                },
                "is_default": True,
                "active": True,
            }
        )

    await _upsert_catalog(
        session,
        ModPolicy.__table__,
        mod_policy_rows,
        conflict_columns=("id",),
        update_columns=("name", "schema_version", "rules", "digest"),
    )
    await _upsert_catalog(
        session,
        RankingPolicy.__table__,
        ranking_policy_rows,
        conflict_columns=("id",),
        update_columns=(
            "code",
            "version",
            "scoreboard_id",
            "metric",
            "tie_breaker",
            "mod_policy_id",
            "configuration",
            "is_default",
            "active",
        ),
    )


async def _seed_community_catalog(session: AsyncSession) -> None:
    permission_ids = {code: item_id for item_id, code, _ in PERMISSIONS}
    channels = (
        ("osu", "#osu", "General discussion.", True, "chat.write"),
        ("announce", "#announce", "Official server announcements.", True, "chat.announce"),
        ("help", "#help", "Gameplay and server help.", True, "chat.write"),
        ("lobby", "#lobby", "Multiplayer lobby discussion.", False, "chat.write"),
    )
    await _upsert_catalog(
        session,
        Channel.__table__,
        tuple(
            {
                "kind": ChannelKind.PUBLIC,
                "slug": slug,
                "name": name,
                "description": description,
                "read_permission_id": permission_ids["chat.read"],
                "write_permission_id": permission_ids[write_permission],
                "manage_permission_id": permission_ids["chat.manage"],
                "auto_join": auto_join,
                "message_length_limit": 2000,
            }
            for slug, name, description, auto_join, write_permission in channels
        ),
        conflict_columns=("slug",),
        update_columns=(
            "kind",
            "name",
            "description",
            "read_permission_id",
            "write_permission_id",
            "manage_permission_id",
            "auto_join",
            "message_length_limit",
        ),
    )


async def _seed_runtime_settings(session: AsyncSession) -> None:
    await _upsert_catalog(
        session,
        ContentSource.__table__,
        (
            {
                "id": 1,
                "code": "osu",
                "name": "osu!",
                "base_url": "https://osu.ppy.sh",
                "official": True,
            },
        ),
        conflict_columns=("id",),
        update_columns=("code", "name", "base_url", "official"),
    )
    await _upsert_catalog(
        session,
        ServerSetting.__table__,
        (
            {
                "key": "stable.protocol",
                "value": {"version": STABLE_PROTOCOL_VERSION},
                "description": "Negotiated Bancho protocol version.",
                "secret": False,
            },
            {
                "key": "stable.codec_limits",
                "value": STABLE_CODEC_LIMITS,
                "description": "Bounds applied while decoding and encoding Stable packets.",
                "secret": False,
            },
        ),
        conflict_columns=("key",),
        update_columns=("value", "description", "secret"),
    )


async def bootstrap_database(session_factory: DbSessionFactory) -> None:
    """Idempotently install records required by every runtime process."""
    async with session_factory.begin() as session:
        await acquire_transaction_lock(session, "database-bootstrap", _BOOTSTRAP_VERSION)
        await _repair_schema(session)
        await _seed_identity(session)
        await _seed_access_catalog(session)
        await _seed_scoring_catalog(session)
        await _seed_community_catalog(session)
        await _seed_runtime_settings(session)


async def _repair_schema(session: AsyncSession) -> None:
    """Apply idempotent repairs that create_all cannot make to existing tables."""
    statements = (
        "ALTER TABLE iam.auth_sessions ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMP WITH TIME ZONE",
        """
        UPDATE iam.auth_sessions
        SET last_activity_at = LEAST(expires_at, GREATEST(created_at, COALESCE(last_activity_at, created_at)))
        WHERE last_activity_at IS NULL
        """,
        "ALTER TABLE iam.auth_sessions ALTER COLUMN last_activity_at SET NOT NULL",
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_auth_sessions_activity_period'
                  AND conrelid = 'iam.auth_sessions'::regclass
            ) THEN
                ALTER TABLE iam.auth_sessions
                ADD CONSTRAINT ck_auth_sessions_activity_period
                CHECK (last_activity_at >= created_at AND last_activity_at <= expires_at);
            END IF;
        END
        $$
        """,
        """
        ALTER TABLE multiplayer.session_presences
        ADD COLUMN IF NOT EXISTS connection_session_id UUID
        """,
        """
        UPDATE multiplayer.session_presences
        SET connection_session_id = id
        WHERE connection_session_id IS NULL
        """,
        """
        ALTER TABLE multiplayer.session_presences
        ALTER COLUMN connection_session_id SET NOT NULL
        """,
        """
        WITH duplicate_presences AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY account_id
                       ORDER BY created_at DESC, id DESC
                   ) AS duplicate_number
            FROM multiplayer.session_presences
            WHERE left_at IS NULL
        )
        UPDATE multiplayer.session_presences AS presence
        SET left_at = GREATEST(CURRENT_TIMESTAMP, presence.created_at + INTERVAL '1 microsecond'),
            leave_reason = 'schema_repair_duplicate'
        FROM duplicate_presences AS duplicate
        WHERE presence.id = duplicate.id
          AND duplicate.duplicate_number > 1
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_session_presences_account_current
        ON multiplayer.session_presences (account_id)
        WHERE left_at IS NULL
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_session_presences_connection
        ON multiplayer.session_presences (connection_session_id, account_id)
        """,
        "ALTER TABLE multiplayer.rounds DROP CONSTRAINT IF EXISTS ck_rounds_single_source",
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_rounds_at_most_one_source'
                  AND conrelid = 'multiplayer.rounds'::regclass
            ) THEN
                ALTER TABLE multiplayer.rounds
                ADD CONSTRAINT ck_rounds_at_most_one_source
                CHECK (num_nonnulls(playlist_revision_id, tournament_pool_item_id) <= 1);
            END IF;
        END
        $$
        """,
        """
        WITH duplicate_rounds AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY session_id
                       ORDER BY started_at DESC NULLS LAST, created_at DESC, id DESC
                   ) AS duplicate_number
            FROM multiplayer.rounds
            WHERE status = 'in_progress'
        )
        UPDATE multiplayer.rounds AS round
        SET status = 'aborted',
            ended_at = CASE
                WHEN round.started_at IS NULL THEN CURRENT_TIMESTAMP
                ELSE GREATEST(CURRENT_TIMESTAMP, round.started_at + INTERVAL '1 microsecond')
            END
        FROM duplicate_rounds AS duplicate
        WHERE round.id = duplicate.id
          AND duplicate.duplicate_number > 1
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_rounds_session_active
        ON multiplayer.rounds (session_id)
        WHERE status = 'in_progress'
        """,
        "ALTER TABLE scoring.leaderboard_entries ALTER COLUMN metric_value TYPE NUMERIC(30, 5)",
        "ALTER TABLE scoring.leaderboard_entries ALTER COLUMN tie_break_value TYPE NUMERIC(30, 5)",
        "ALTER TABLE scoring.replays DROP CONSTRAINT IF EXISTS uq_replays_sha256",
        "ALTER TABLE scoring.replays DROP CONSTRAINT IF EXISTS uq_replays_storage_key",
        "ALTER TABLE content.rating_votes ADD COLUMN IF NOT EXISTS beatmap_id BIGINT",
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'content'
                  AND table_name = 'rating_votes'
                  AND column_name = 'beatmap_revision_id'
            ) THEN
                EXECUTE 'UPDATE content.rating_votes AS vote '
                        'SET beatmap_id = revision.beatmap_id '
                        'FROM content.beatmap_revisions AS revision '
                        'WHERE vote.beatmap_revision_id = revision.id AND vote.beatmap_id IS NULL';
            END IF;
        END
        $$
        """,
        "ALTER TABLE content.rating_votes DROP CONSTRAINT IF EXISTS ck_rating_votes_single_target",
        "DROP INDEX IF EXISTS content.uq_rating_votes_revision_account",
        "DROP INDEX IF EXISTS content.ix_rating_votes_revision_rating",
        "ALTER TABLE content.rating_votes DROP COLUMN IF EXISTS beatmap_revision_id",
        """
        DELETE FROM content.rating_votes AS vote
        WHERE vote.beatmap_id IS NOT NULL
          AND vote.id NOT IN (
              SELECT DISTINCT ON (account_id, beatmap_id) id
              FROM content.rating_votes
              WHERE beatmap_id IS NOT NULL
              ORDER BY account_id, beatmap_id, updated_at DESC, id DESC
          )
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_rating_votes_beatmap_id_beatmaps'
                  AND conrelid = 'content.rating_votes'::regclass
            ) THEN
                ALTER TABLE content.rating_votes
                ADD CONSTRAINT fk_rating_votes_beatmap_id_beatmaps
                FOREIGN KEY (beatmap_id) REFERENCES content.beatmaps(id) ON DELETE RESTRICT;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_rating_votes_single_target'
                  AND conrelid = 'content.rating_votes'::regclass
            ) THEN
                ALTER TABLE content.rating_votes
                ADD CONSTRAINT ck_rating_votes_single_target
                CHECK (num_nonnulls(beatmapset_id, beatmap_id) = 1);
            END IF;
        END
        $$
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_rating_votes_beatmap_account
        ON content.rating_votes (account_id, beatmap_id)
        WHERE beatmap_id IS NOT NULL
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_rating_votes_beatmap_rating
        ON content.rating_votes (beatmap_id, rating)
        """,
    )
    for statement in statements:
        await session.execute(text(statement))
