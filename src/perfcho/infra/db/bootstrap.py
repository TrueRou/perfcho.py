"""Seed the deterministic minimum PostgreSQL runtime catalog."""

import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import FromClause

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import (
    AccountStatus,
    AccountType,
    CalculationKind,
    ChannelKind,
    Ruleset,
)
from perfcho.infra.db.locks import acquire_transaction_lock
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
from perfcho.infra.db.models.iam import OAuthClient, OAuthClientScope, OAuthClientSecret, Scope
from perfcho.infra.db.models.scoring import (
    CalculationFormula,
    CalculationRelease,
    RankingPolicy,
)
from perfcho.infra.security.tokens import digest_opaque_token
from perfcho.infra.settings import settings

STABLE_PROTOCOL_VERSION = 19

_BOOTSTRAP_VERSION = 1
_BOOTSTRAP_EPOCH = datetime(2007, 9, 16, tzinfo=UTC)
_BOOTSTRAP_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://perfcho.dev/database-bootstrap")
_DEFAULT_CALCULATOR = "perfcho-pp"
_DEFAULT_PERFORMANCE_FORMULA_CODE = "official"
_DEFAULT_DIFFICULTY_FORMULA_CODE = "official-difficulty"
_DEFAULT_RELEASE_VERSION = "2026.07.1"
_EMPTY_TABLE_CACHE_KEY = "perfcho.bootstrap.empty_tables"
_LAZER_CLIENT_KEY = "5"
_LAZER_CLIENT_SECRET = "FGc9GAtyHzeQDshWP5Ah7dega8hJACAJpQtw6OXk"

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
        pg_get_serial_sequence('core.account', 'id'),
        GREATEST(
            2,
            (SELECT COALESCE(MAX(id), 0) FROM core.account),
            COALESCE(
                (
                    SELECT last_value
                    FROM pg_sequences
                    WHERE schemaname = 'core'
                      AND sequencename = split_part(
                          pg_get_serial_sequence('core.account', 'id'),
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


def _bootstrap_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(_BOOTSTRAP_NAMESPACE, name)


async def _upsert_catalog(
    session: AsyncSession,
    table: FromClause,
    rows: Sequence[Mapping[str, object]],
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] = (),
) -> bool:
    if not await _table_is_empty(session, table):
        return False

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
    return True


async def _table_is_empty(session: AsyncSession, table: FromClause) -> bool:
    """Return the initial emptiness of a catalog table for this bootstrap transaction."""
    empty_tables = cast(dict[str, bool], session.info.setdefault(_EMPTY_TABLE_CACHE_KEY, {}))
    table_name = str(table)
    if table_name not in empty_tables:
        empty_tables[table_name] = await session.scalar(select(1).select_from(table).limit(1)) is None
    return empty_tables[table_name]


def _player_policy_configuration(ruleset: Ruleset) -> dict[str, object]:
    return {
        "metric": "pp",
        "tie_breaker": "ended_at",
        "calculation_release_id": str(_bootstrap_uuid(f"calculation-release:performance:{ruleset.value}")),
        "selection": {"best_per_account": True},
        "eligibility": {
            "beatmap_statuses": ["ranked", "approved", "qualified", "loved"],
            "mods": {
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
            },
        },
    }


def _default_release_configuration(kind: CalculationKind, ruleset: Ruleset) -> dict[str, object]:
    """Describe the built-in release identity sent to the calculator."""
    return {
        "source": _DEFAULT_PERFORMANCE_FORMULA_CODE,
        "calculator": _DEFAULT_CALCULATOR,
        "kind": kind.value,
        "ruleset": ruleset.value,
        "bootstrap_version": _BOOTSTRAP_VERSION,
    }


async def _seed_identity(session: AsyncSession) -> None:
    account_seeded = await _upsert_catalog(
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
    if account_seeded:
        await session.execute(_ACCOUNT_IDENTITY_SQL)

    if await _table_is_empty(session, AccountName.__table__):
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
    seeded_lazer_client_id = _bootstrap_uuid("oauth-client:lazer")
    lazer_client_id = await session.scalar(
        insert(OAuthClient)
        .values(
            id=seeded_lazer_client_id,
            client_key=_LAZER_CLIENT_KEY,
            name="osu!lazer",
            owner_account_id=None,
            is_confidential=False,
            first_party=True,
            active=True,
            created_at=_BOOTSTRAP_EPOCH,
            updated_at=_BOOTSTRAP_EPOCH,
        )
        .on_conflict_do_update(
            index_elements=(OAuthClient.client_key,),
            set_={
                "name": "osu!lazer",
                "is_confidential": False,
                "first_party": True,
                "active": True,
                "updated_at": _BOOTSTRAP_EPOCH,
            },
        )
        .returning(OAuthClient.id)
    )
    if lazer_client_id is None:
        raise RuntimeError("database did not return the bootstrapped lazer OAuth client")
    await session.execute(
        insert(OAuthClientSecret)
        .values(
            id=_bootstrap_uuid("oauth-client-secret:lazer:production"),
            client_id=lazer_client_id,
            secret_digest=digest_opaque_token(
                _LAZER_CLIENT_SECRET,
                key=settings.token_hmac_key.get_secret_value().encode(),
            ),
            secret_prefix=_LAZER_CLIENT_SECRET[:16],
            created_at=_BOOTSTRAP_EPOCH,
            expires_at=None,
            revoked_at=None,
        )
        .on_conflict_do_update(
            index_elements=(OAuthClientSecret.id,),
            set_={
                "client_id": lazer_client_id,
                "secret_digest": digest_opaque_token(
                    _LAZER_CLIENT_SECRET,
                    key=settings.token_hmac_key.get_secret_value().encode(),
                ),
                "secret_prefix": _LAZER_CLIENT_SECRET[:16],
                "expires_at": None,
                "revoked_at": None,
            },
        )
    )
    await session.execute(
        insert(OAuthClientScope)
        .values([{"client_id": lazer_client_id, "scope_id": item_id} for item_id, *_ in OAUTH_SCOPES])
        .on_conflict_do_nothing(index_elements=(OAuthClientScope.client_id, OAuthClientScope.scope_id))
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
    await _seed_default_calculations(session)
    await _upsert_catalog(
        session,
        RankingPolicy.__table__,
        tuple(
            {
                "id": _bootstrap_uuid(f"ranking-policy:player:{ruleset.value}"),
                "code": f"player.{ruleset.value}",
                "ruleset": ruleset,
                "active": True,
                "configuration": _player_policy_configuration(ruleset),
            }
            for ruleset in Ruleset
        ),
        conflict_columns=("id",),
        update_columns=("code", "ruleset", "active", "configuration"),
    )


async def _seed_default_calculations(session: AsyncSession) -> None:
    """Install the default Perfcho PP catalog for vanilla rulesets."""
    difficulty_formula_id = _bootstrap_uuid(f"calculation-formula:{_DEFAULT_DIFFICULTY_FORMULA_CODE}")
    performance_formula_id = _bootstrap_uuid(f"calculation-formula:{_DEFAULT_PERFORMANCE_FORMULA_CODE}")
    await _upsert_catalog(
        session,
        CalculationFormula.__table__,
        (
            {
                "id": difficulty_formula_id,
                "code": _DEFAULT_DIFFICULTY_FORMULA_CODE,
                "name": "Perfcho Difficulty",
                "kind": CalculationKind.DIFFICULTY,
                "calculator": _DEFAULT_CALCULATOR,
                "description": "Default difficulty formula used by perfcho-pp.",
                "enabled": True,
            },
            {
                "id": performance_formula_id,
                "code": _DEFAULT_PERFORMANCE_FORMULA_CODE,
                "name": "Perfcho PP",
                "kind": CalculationKind.PERFORMANCE,
                "calculator": _DEFAULT_CALCULATOR,
                "description": "Default performance formula for vanilla rulesets.",
                "enabled": True,
            },
        ),
        conflict_columns=("id",),
        update_columns=("code", "name", "kind", "calculator", "description", "enabled"),
    )
    for ruleset in Ruleset:
        difficulty_release_id = _bootstrap_uuid(f"calculation-release:difficulty:{ruleset.value}")
        performance_release_id = _bootstrap_uuid(f"calculation-release:performance:{ruleset.value}")
        difficulty_configuration = _default_release_configuration(CalculationKind.DIFFICULTY, ruleset)
        performance_configuration = _default_release_configuration(CalculationKind.PERFORMANCE, ruleset)
        await _upsert_catalog(
            session,
            CalculationRelease.__table__,
            (
                {
                    "id": difficulty_release_id,
                    "formula_id": difficulty_formula_id,
                    "ruleset": ruleset,
                    "version": _DEFAULT_RELEASE_VERSION,
                    "configuration": difficulty_configuration,
                    "active": True,
                },
            ),
            conflict_columns=("id",),
            update_columns=(
                "formula_id",
                "ruleset",
                "version",
                "configuration",
                "active",
            ),
        )
        await _upsert_catalog(
            session,
            CalculationRelease.__table__,
            (
                {
                    "id": performance_release_id,
                    "formula_id": performance_formula_id,
                    "ruleset": ruleset,
                    "version": _DEFAULT_RELEASE_VERSION,
                    "configuration": performance_configuration,
                    "difficulty_release_id": difficulty_release_id,
                    "active": True,
                },
            ),
            conflict_columns=("id",),
            update_columns=(
                "formula_id",
                "ruleset",
                "version",
                "configuration",
                "difficulty_release_id",
                "active",
            ),
        )


async def _seed_community_catalog(session: AsyncSession) -> None:
    permission_ids = {code: item_id for item_id, code, _ in PERMISSIONS}
    channels = (
        ("osu", "osu", "General discussion.", True, "chat.write"),
        ("announce", "announce", "Official server announcements.", True, "chat.announce"),
        ("help", "help", "Gameplay and server help.", True, "chat.write"),
        ("lobby", "lobby", "Multiplayer lobby discussion.", False, "chat.write"),
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


async def _seed_content_source(session: AsyncSession) -> None:
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


async def bootstrap_database(session_factory: DbSessionFactory) -> None:
    """Idempotently install records required by every runtime process."""
    async with session_factory.begin() as session:
        await acquire_transaction_lock(session, "database-bootstrap", _BOOTSTRAP_VERSION)
        await _seed_identity(session)
        await _seed_access_catalog(session)
        await _seed_scoring_catalog(session)
        await _seed_community_catalog(session)
        await _seed_content_source(session)
