import asyncio

import pytest
from sqlalchemy import UniqueConstraint, delete, func, select, text

from perfcho.infra.db import DbBase
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.bootstrap import (
    OAUTH_SCOPES,
    PERMISSIONS,
    ROLE_PERMISSION_CODES,
    ROLES,
    STABLE_CODEC_LIMITS,
    STABLE_PROTOCOL_VERSION,
    _bootstrap_uuid,
    _player_policy_configuration,
    bootstrap_database,
)
from perfcho.infra.db.enums import AccountStatus, AccountType, CalculationKind, ChannelKind, Ruleset
from perfcho.infra.db.models.authz import AccountRoleGrant, Entitlement, Permission, Role, RolePermission
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


def test_bootstrap_catalog_has_stable_ids_and_complete_role_links() -> None:
    assert [item_id for item_id, *_ in OAUTH_SCOPES] == list(range(1, len(OAUTH_SCOPES) + 1))
    assert [item_id for item_id, *_ in PERMISSIONS] == list(range(1, len(PERMISSIONS) + 1))
    permission_codes = {code for _, code, _ in PERMISSIONS}
    assert {code.split(".", 1)[0] for code in permission_codes} >= {
        "account",
        "chat",
        "content",
        "scoring",
        "multiplayer",
        "moderation",
        "admin",
    }
    assert set(ROLE_PERMISSION_CODES) == {code for _, code, *_ in ROLES}
    assert all(codes <= permission_codes for codes in ROLE_PERMISSION_CODES.values())
    assert "admin.access" not in ROLE_PERMISSION_CODES["user"]
    assert "admin.access" not in ROLE_PERMISSION_CODES["bot"]
    assert ROLE_PERMISSION_CODES["administrator"] == permission_codes


def test_bootstrap_protocol_policy_and_uuid_contracts() -> None:
    assert STABLE_PROTOCOL_VERSION == 19
    assert STABLE_CODEC_LIMITS == {
        "max_body_size": 16 * 1024 * 1024,
        "max_packet_size": 2 * 1024 * 1024,
        "max_packet_count": 4096,
        "max_string_size": 64 * 1024,
        "max_list_length": 8192,
        "max_frame_count": 4096,
        "max_uleb128_bytes": 5,
    }

    identifier = _bootstrap_uuid("unit-test")
    assert identifier.version == 5
    assert identifier == _bootstrap_uuid("unit-test")

    for ruleset in Ruleset:
        configuration = _player_policy_configuration(ruleset)
        assert configuration["metric"] == "pp"
        assert configuration["tie_breaker"] == "ended_at"
        assert configuration["selection"] == {"best_per_account": True}
        assert configuration["calculation_release_id"] == str(
            _bootstrap_uuid(f"calculation-release:performance:{ruleset.value}")
        )
        assert configuration["eligibility"] == {
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
        }


def test_bootstrap_models_expose_every_conflict_key() -> None:
    assert Account.__table__.c.id.primary_key
    assert not AccountName.__table__.c.account_id.nullable
    assert UserProfile.__table__.c.account_id.primary_key
    assert UserPreference.__table__.c.account_id.primary_key
    assert Scope.__table__.c.id.primary_key
    assert Permission.__table__.c.id.primary_key
    assert Role.__table__.c.id.primary_key
    assert RolePermission.__table__.c.role_id.primary_key
    assert RolePermission.__table__.c.permission_id.primary_key
    assert Entitlement.__table__.c.id.primary_key
    assert ContentSource.__table__.c.id.primary_key
    assert RankingPolicy.__table__.c.id.primary_key
    channel_table = DbBase.metadata.tables["community.channel"]
    assert any(
        isinstance(constraint, UniqueConstraint) and tuple(constraint.columns.keys()) == ("slug",)
        for constraint in channel_table.constraints
    )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_bootstrap_is_concurrent_and_repeatably_idempotent(postgres_database_url: str) -> None:
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    try:
        await asyncio.gather(bootstrap_database(session_factory), bootstrap_database(session_factory))
        await bootstrap_database(session_factory)

        async with session_factory() as session:
            account = await session.get(Account, 1)
            assert account is not None
            assert account.type is AccountType.BOT
            assert account.status is AccountStatus.ACTIVE

            current_name = await session.scalar(
                select(AccountName).where(AccountName.account_id == 1, AccountName.ended_at.is_(None))
            )
            assert current_name is not None
            assert current_name.display_name == "BanchoBot"
            assert await session.get(UserProfile, 1) is not None
            assert await session.get(UserPreference, 1) is not None

            assert await session.scalar(select(func.count()).select_from(Scope)) == len(OAUTH_SCOPES)
            lazer_client = await session.scalar(select(OAuthClient).where(OAuthClient.client_key == "5"))
            assert lazer_client is not None
            assert lazer_client.first_party is True
            assert lazer_client.active is True
            assert await session.scalar(
                select(func.count()).select_from(OAuthClientScope).where(OAuthClientScope.client_id == lazer_client.id)
            ) == len(OAUTH_SCOPES)
            lazer_secret = await session.scalar(
                select(OAuthClientSecret).where(OAuthClientSecret.client_id == lazer_client.id)
            )
            assert lazer_secret is not None
            assert lazer_secret.secret_digest == digest_opaque_token(
                "FGc9GAtyHzeQDshWP5Ah7dega8hJACAJpQtw6OXk",
                key=settings.token_hmac_key.get_secret_value().encode(),
            )
            assert await session.scalar(select(func.count()).select_from(Permission)) == len(PERMISSIONS)
            assert await session.scalar(select(func.count()).select_from(Role)) == len(ROLES)
            assert await session.scalar(select(func.count()).select_from(RolePermission)) == sum(
                len(codes) for codes in ROLE_PERMISSION_CODES.values()
            )
            assert await session.scalar(select(func.count()).select_from(AccountRoleGrant)) == 1
            bot_grant = await session.scalar(select(AccountRoleGrant))
            assert bot_grant is not None
            assert bot_grant.account_id == 1
            assert bot_grant.id.version == 5
            assert await session.scalar(select(func.count()).select_from(Entitlement)) == 1

            assert await session.scalar(select(func.count()).select_from(ContentSource)) == 1
            assert await session.scalar(select(func.count()).select_from(RankingPolicy)) == len(Ruleset)
            assert await session.scalar(select(func.count()).select_from(CalculationFormula)) == 2
            assert await session.scalar(select(func.count()).select_from(CalculationRelease)) == len(Ruleset) * 2
            assert await session.scalar(
                select(func.count()).select_from(RankingPolicy).where(RankingPolicy.active.is_(True))
            ) == len(Ruleset)

            channels = (await session.scalars(select(Channel).order_by(Channel.slug))).all()
            assert {channel.name for channel in channels} == {"osu", "announce", "help", "lobby"}
            assert all(channel.kind is ChannelKind.PUBLIC for channel in channels)

            policies = (await session.scalars(select(RankingPolicy))).all()
            assert {policy.ruleset for policy in policies} == set(Ruleset)
            assert all(policy.id.version == 5 for policy in policies)
            assert all(policy.code == f"player.{policy.ruleset.value}" for policy in policies)
            assert all(policy.configuration == _player_policy_configuration(policy.ruleset) for policy in policies)
            formulas = {formula.code: formula for formula in await session.scalars(select(CalculationFormula))}
            assert set(formulas) == {"official", "official-difficulty"}
            assert formulas["official"].calculator == "perfcho-pp"
            assert formulas["official"].kind is CalculationKind.PERFORMANCE
            assert formulas["official"].enabled is True
            assert formulas["official-difficulty"].calculator == "perfcho-pp"
            assert formulas["official-difficulty"].kind is CalculationKind.DIFFICULTY
            assert formulas["official-difficulty"].enabled is True

            releases = (await session.scalars(select(CalculationRelease))).all()
            assert all(release.active for release in releases)
            assert all(release.version == "2026.07.1" for release in releases)
            performance_releases = [release for release in releases if release.formula_id == formulas["official"].id]
            difficulty_releases = [
                release for release in releases if release.formula_id == formulas["official-difficulty"].id
            ]
            assert {release.ruleset for release in performance_releases} == set(Ruleset)
            assert {release.ruleset for release in difficulty_releases} == set(Ruleset)
            assert all(release.difficulty_release_id is not None for release in performance_releases)
            assert all(release.difficulty_release_id is None for release in difficulty_releases)
            difficulty_by_ruleset = {release.ruleset: release.id for release in difficulty_releases}
            assert all(
                release.difficulty_release_id == difficulty_by_ruleset[release.ruleset]
                for release in performance_releases
            )
            assert not await session.scalar(
                select(func.count())
                .select_from(AccountRoleGrant)
                .join(Role, Role.id == AccountRoleGrant.role_id)
                .where(Role.code == "administrator")
            )

            sequence_value = await session.scalar(
                text(
                    "SELECT last_value FROM pg_sequences "
                    "WHERE schemaname = 'core' AND sequencename = "
                    "split_part(pg_get_serial_sequence('core.account', 'id'), '.', 2)"
                )
            )
            assert sequence_value is not None and sequence_value >= 2
    finally:
        await db_engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_bootstrap_only_seeds_empty_tables(postgres_database_url: str) -> None:
    del postgres_database_url
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    try:
        async with session_factory.begin() as session:
            account_name = await session.scalar(select(AccountName).where(AccountName.account_id == 1))
            assert account_name is not None
            account_name.display_name = "CustomBot"
            account_name.name_key = "custombot"
            await session.execute(delete(ContentSource))

        await bootstrap_database(session_factory)

        async with session_factory() as session:
            account_name = await session.scalar(select(AccountName).where(AccountName.account_id == 1))
            assert account_name is not None
            assert account_name.display_name == "CustomBot"
            assert account_name.name_key == "custombot"

            content_source = await session.get(ContentSource, 1)
            assert content_source is not None
            assert content_source.code == "osu"
    finally:
        await db_engine.dispose()
