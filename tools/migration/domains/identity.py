"""Migrate bancho.py accounts, authorization, devices, and login evidence."""

from __future__ import annotations

import hashlib
import ipaddress
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import bindparam, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import AccountStatus, AccountType, ClientFamily, GrantEffect, Ruleset, SanctionKind
from perfcho.infra.db.models.audit import AuditEvent
from perfcho.infra.db.models.authz import (
    AccountEntitlementGrant,
    AccountPermissionGrant,
    AccountRoleGrant,
    Entitlement,
    Permission,
    Role,
)
from perfcho.infra.db.models.core import Account, AccountEmail, AccountName, UserPreference, UserProfile
from perfcho.infra.db.models.iam import (
    AccountDevice,
    Device,
    DeviceIdentifier,
    PasswordCredential,
)
from perfcho.infra.db.models.moderation import ModerationCase, Sanction
from perfcho.infra.security.tokens import digest_device_component, hmac_sha256_digest
from perfcho.infra.settings import settings
from perfcho.modules.common.normalization import normalize_email, normalize_name
from tools.migration.domains.common import run_batched_phase, run_single_phase
from tools.migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.migration.transforms import aware_datetime, play_styles, source_ruleset, unix_datetime

_UNRESTRICTED = 1 << 0
_VERIFIED = 1 << 1
_WHITELISTED = 1 << 2
_SUPPORTER = 1 << 4
_PREMIUM = 1 << 5
_ALUMNI = 1 << 7
_TOURNEY_MANAGER = 1 << 10
_NOMINATOR = 1 << 11
_MODERATOR = 1 << 12
_ADMINISTRATOR = 1 << 13
_DEVELOPER = 1 << 14
_DONATOR = _SUPPORTER | _PREMIUM
_MAPPED_PRIVILEGES = _UNRESTRICTED | _DONATOR | _TOURNEY_MANAGER | _NOMINATOR | _MODERATOR | _ADMINISTRATOR | _DEVELOPER
_KNOWN_UNSUPPORTED_PRIVILEGES = _VERIFIED | _WHITELISTED | _ALUMNI
_DEVICE_COMPONENTS = (
    ("path", "osupath"),
    ("adapters_md5", "adapters"),
    ("uninstall", "uninstall_id"),
    ("disk", "disk_serial"),
)


@dataclass(frozen=True, slots=True)
class _PreparedUser:
    source_id: int
    display_name: str | None
    name_key: str | None
    email: str | None
    email_key: str | None
    registered_at: datetime
    last_seen_at: datetime
    country_code: str | None
    default_ruleset: Ruleset
    play_style: list[str]
    bio: str | None
    privilege_mask: int
    password_verifier: str | None
    silence_ends_at: datetime | None
    supporter_ends_at: datetime | None
    metadata: dict[str, object]
    skip: bool
    override_target_id: int | None

    @property
    def identity_is_valid(self) -> bool:
        return (
            self.display_name is not None
            and self.name_key is not None
            and self.email is not None
            and self.email_key is not None
        )


@dataclass(frozen=True, slots=True)
class _PreparedDevice:
    account_id: int
    proposed_id: object
    fingerprint_hmac: bytes
    components: tuple[tuple[str, bytes], ...]
    seen_at: datetime
    occurrences: int


async def migrate_identity(runtime: MigrationRuntime) -> None:
    """Migrate canonical account identity, access, device, and authentication facts."""
    await _reconstruct_account_mappings(runtime)

    async def users_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_user_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="identity.users",
        table="users",
        key="id",
        handler=users_handler,
    )

    async def sequence_handler(session: AsyncSession) -> None:
        await session.execute(
            text(
                """
                SELECT setval(
                    pg_get_serial_sequence('core.account', 'id'),
                    GREATEST(2, (SELECT COALESCE(MAX(id), 0) FROM core.account)),
                    true
                )
                """
            )
        )

    await run_single_phase(runtime, phase="identity.account_sequence", handler=sequence_handler)

    async def attempts_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_auth_attempt_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="identity.auth_attempts",
        table="ingame_logins",
        key="id",
        handler=attempts_handler,
    )

    async def audit_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_audit_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="identity.audit_logs",
        table="logs",
        key="id",
        handler=audit_handler,
    )


async def _reconstruct_account_mappings(runtime: MigrationRuntime) -> None:
    for rows in runtime.source.iter_batches(
        "users",
        key="id",
        batch_size=runtime.config.batch_size,
        columns=("id", "name", "email"),
    ):
        runtime.mappings.source_account_ids.update(_positive_int(row.get("id"), "user id") for row in rows)
        prepared = [_prepare_user(runtime, row, identity_only=True) for row in rows]
        async with runtime.session_factory() as session:
            await _resolve_accounts(session, runtime, prepared, allow_create=False, diagnose=False)


async def _migrate_user_batch(session: AsyncSession, runtime: MigrationRuntime, rows: list[SourceRow]) -> None:
    prepared = [_prepare_user(runtime, row) for row in rows]
    resolved, imported = await _resolve_accounts(session, runtime, prepared, allow_create=True, diagnose=True)
    if not resolved:
        return

    by_source = {user.source_id: user for user in prepared}
    await _insert_account_graph(session, runtime, resolved, imported, by_source)
    await _insert_access_facts(session, runtime, resolved, by_source)
    await _insert_moderation_facts(session, runtime, resolved, by_source)
    await _migrate_client_hashes(session, runtime, resolved)


def _prepare_user(runtime: MigrationRuntime, row: SourceRow, *, identity_only: bool = False) -> _PreparedUser:
    source_id = _positive_int(row.get("id"), "user id")
    override = runtime.overrides.accounts.get(source_id)
    skip = override.skip if override is not None else False
    override_target_id = override.target_account_id if override is not None else None

    display_name: str | None = None
    name_key: str | None = None
    email: str | None = None
    email_key: str | None = None
    try:
        raw_name = (
            override.display_name if override is not None and override.display_name is not None else row.get("name")
        )
        if not isinstance(raw_name, str):
            raise ValueError("name must be text")
        display_name = unicodedata.normalize("NFKC", raw_name).strip()
        name_key = normalize_name(display_name)
    except ValueError as error:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_name_malformed",
            str(error),
            entity="users",
            source_id=source_id,
        )
    try:
        raw_email = override.email if override is not None and override.email is not None else row.get("email")
        if not isinstance(raw_email, str):
            raise ValueError("email must be text")
        email = raw_email.strip()
        email_key = normalize_email(email)
    except ValueError as error:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_email_malformed",
            str(error),
            entity="users",
            source_id=source_id,
        )

    if identity_only:
        return _PreparedUser(
            source_id,
            display_name,
            name_key,
            email,
            email_key,
            runtime.started_at,
            runtime.started_at,
            None,
            Ruleset.OSU,
            [],
            None,
            0,
            None,
            None,
            None,
            {},
            skip,
            override_target_id,
        )

    registered_at = _source_timestamp(runtime, row.get("creation_time"))
    last_seen_at = _source_timestamp(runtime, row.get("latest_activity"))
    country_code = _country_code(row.get("country"))
    if row.get("country") not in {None, ""} and country_code is None:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_country_malformed",
            "country code was not a two-letter value and was omitted",
            entity="users",
            source_id=source_id,
        )

    try:
        default_ruleset = source_ruleset(row.get("preferred_mode"))
    except ValueError as error:
        default_ruleset = Ruleset.OSU
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_ruleset_malformed",
            str(error),
            entity="users",
            source_id=source_id,
        )
    try:
        style = play_styles(row.get("play_style"))
    except ValueError as error:
        style = []
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_play_style_malformed",
            str(error),
            entity="users",
            source_id=source_id,
        )

    privilege_mask = row.get("priv")
    if isinstance(privilege_mask, bool) or not isinstance(privilege_mask, int) or privilege_mask < 0:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_privileges_malformed",
            "privilege mask was not a nonnegative integer and was treated as zero",
            entity="users",
            source_id=source_id,
        )
        privilege_mask = 0

    password_verifier = _password_verifier(row.get("pw_bcrypt"))
    if row.get("pw_bcrypt") not in {None, ""} and password_verifier is None:
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "user_password_malformed",
            "legacy bcrypt verifier was malformed and was not imported",
            entity="users",
            source_id=source_id,
        )

    badge_name = row.get("custom_badge_name")
    badge_icon = row.get("custom_badge_icon")
    raw_api_key = row.get("api_key")
    api_metadata: dict[str, object] = {"present": bool(raw_api_key)}
    if isinstance(raw_api_key, str) and raw_api_key:
        api_metadata["sha256"] = hashlib.sha256(raw_api_key.encode()).hexdigest()
    unsupported = privilege_mask & ~_MAPPED_PRIVILEGES
    metadata: dict[str, object] = {
        "migration": {
            "source_user_id": source_id,
            "privilege_mask": privilege_mask,
            "unsupported_privilege_bits": unsupported,
            "known_unsupported_privilege_bits": privilege_mask & _KNOWN_UNSUPPORTED_PRIVILEGES,
            "custom_badge": {
                "name": badge_name if isinstance(badge_name, str) and badge_name else None,
                "icon": badge_icon if isinstance(badge_icon, str) and badge_icon else None,
            },
            "api_key": api_metadata,
        }
    }
    bio = row.get("userpage_content")
    return _PreparedUser(
        source_id=source_id,
        display_name=display_name,
        name_key=name_key,
        email=email,
        email_key=email_key,
        registered_at=registered_at,
        last_seen_at=max(registered_at, last_seen_at),
        country_code=country_code,
        default_ruleset=default_ruleset,
        play_style=style,
        bio=bio if isinstance(bio, str) and bio else None,
        privilege_mask=privilege_mask,
        password_verifier=password_verifier,
        silence_ends_at=_optional_unix_timestamp(runtime, row.get("silence_end")),
        supporter_ends_at=_optional_unix_timestamp(runtime, row.get("donor_end")),
        metadata=metadata,
        skip=skip,
        override_target_id=override_target_id,
    )


async def _resolve_accounts(
    session: AsyncSession,
    runtime: MigrationRuntime,
    users: list[_PreparedUser],
    *,
    allow_create: bool,
    diagnose: bool,
) -> tuple[dict[int, int], set[int]]:
    active_users = [user for user in users if not user.skip]
    runtime.mappings.source_account_ids.update(user.source_id for user in users)
    name_keys = {user.name_key for user in active_users if user.name_key is not None}
    email_keys = {user.email_key for user in active_users if user.email_key is not None}
    candidate_ids = {user.source_id for user in active_users}
    candidate_ids.update(user.override_target_id for user in active_users if user.override_target_id is not None)

    names = {
        key: account_id
        for key, account_id in (
            await session.execute(
                select(AccountName.name_key, AccountName.account_id).where(
                    AccountName.name_key.in_(name_keys), AccountName.ended_at.is_(None)
                )
            )
        ).all()
    }
    emails = {
        key: account_id
        for key, account_id in (
            await session.execute(
                select(AccountEmail.email_key, AccountEmail.account_id).where(
                    AccountEmail.email_key.in_(email_keys), AccountEmail.retired_at.is_(None)
                )
            )
        ).all()
    }
    existing_ids: set[int] = set((await session.scalars(select(Account.id).where(Account.id.in_(candidate_ids)))).all())
    reverse_mappings = {target_id: source_id for source_id, target_id in runtime.mappings.accounts.items()}
    resolved: dict[int, int] = {}
    imported: set[int] = set()

    for user in active_users:
        target_id: int | None = None
        if user.override_target_id is not None:
            if user.override_target_id not in existing_ids:
                if diagnose:
                    _account_error(
                        runtime,
                        "account_override_missing",
                        "account override references a missing target account",
                        user.source_id,
                        {"target_account_id": user.override_target_id},
                    )
                continue
            target_id = user.override_target_id
        else:
            name_target = names.get(user.name_key) if user.name_key is not None else None
            email_target = emails.get(user.email_key) if user.email_key is not None else None
            id_target = user.source_id if user.source_id in existing_ids else None
            natural_targets = {candidate for candidate in (name_target, email_target) if candidate is not None}
            if len(natural_targets) > 1:
                if diagnose:
                    _account_error(
                        runtime,
                        "account_resolution_ambiguous",
                        "current name, email, and source ID resolve to different target accounts",
                        user.source_id,
                        {"target_account_ids": sorted(natural_targets)},
                    )
                continue
            natural_target = next(iter(natural_targets), None)
            if natural_target is not None and id_target is not None and natural_target != id_target:
                if name_target is not None and email_target is not None and name_target == email_target:
                    target_id = natural_target
                else:
                    if diagnose:
                        _account_error(
                            runtime,
                            "account_resolution_ambiguous",
                            "one natural identifier and the source ID resolve to different target accounts",
                            user.source_id,
                            {"natural_target_id": natural_target, "id_target_id": id_target},
                        )
                    continue
            elif natural_target is not None:
                target_id = natural_target
            elif id_target is not None:
                if not allow_create:
                    continue
                target_id = await _next_account_id(
                    session,
                    reserved=(runtime.mappings.source_account_ids | set(reverse_mappings) | set(resolved.values())),
                )
                imported.add(user.source_id)
                runtime.report.add(
                    DiagnosticSeverity.WARNING,
                    "account_id_remapped",
                    "source account ID was occupied by unrelated target data",
                    entity="users",
                    source_id=user.source_id,
                    details={"target_account_id": target_id},
                )

        if target_id is None:
            if not allow_create:
                continue
            if not user.identity_is_valid:
                if diagnose:
                    _account_error(
                        runtime,
                        "account_identity_malformed",
                        "a new account requires a valid name and email",
                        user.source_id,
                    )
                continue
            if user.source_id <= 2_147_483_647:
                target_id = user.source_id
            else:
                target_id = await _next_account_id(
                    session,
                    reserved=(runtime.mappings.source_account_ids | set(reverse_mappings) | set(resolved.values())),
                )
                runtime.report.add(
                    DiagnosticSeverity.WARNING,
                    "account_id_remapped",
                    "source account ID exceeds Stable's signed 32-bit range",
                    entity="users",
                    source_id=user.source_id,
                    details={"target_account_id": target_id},
                )
            imported.add(user.source_id)

        previous_source = reverse_mappings.get(target_id)
        if previous_source is not None and previous_source != user.source_id:
            if diagnose:
                _account_error(
                    runtime,
                    "account_resolution_ambiguous",
                    "multiple source accounts resolve to the same target account",
                    user.source_id,
                    {"target_account_id": target_id, "other_source_id": previous_source},
                )
            continue
        runtime.mappings.accounts[user.source_id] = target_id
        reverse_mappings[target_id] = user.source_id
        resolved[user.source_id] = target_id
        runtime.report.increment("identity.users", "resolved" if user.source_id not in imported else "imported")

    for user in users:
        if user.skip:
            runtime.report.increment("identity.users", "skipped_override")
    return resolved, imported


async def _insert_account_graph(
    session: AsyncSession,
    runtime: MigrationRuntime,
    resolved: dict[int, int],
    imported: set[int],
    users: dict[int, _PreparedUser],
) -> None:
    new_accounts = [
        {
            "id": resolved[source_id],
            "type": AccountType.USER,
            "status": AccountStatus.ACTIVE,
            "country_code": users[source_id].country_code,
            "registered_at": users[source_id].registered_at,
            "activated_at": users[source_id].registered_at,
            "last_seen_at": users[source_id].last_seen_at,
            "auth_version": 1,
        }
        for source_id in imported
    ]
    if new_accounts:
        await session.execute(insert(Account).values(new_accounts).on_conflict_do_nothing())

    valid = [
        (source_id, target_id, users[source_id])
        for source_id, target_id in resolved.items()
        if users[source_id].identity_is_valid
    ]
    if valid:
        await session.execute(
            insert(AccountName)
            .values(
                [
                    {
                        "account_id": target_id,
                        "display_name": user.display_name,
                        "name_key": user.name_key,
                        "started_at": user.registered_at,
                    }
                    for _, target_id, user in valid
                ]
            )
            .on_conflict_do_nothing()
        )
        await session.execute(
            insert(AccountEmail)
            .values(
                [
                    {
                        "id": runtime.ids.make("account-email", source_id),
                        "account_id": target_id,
                        "email": user.email,
                        "email_key": user.email_key,
                        "is_primary": True,
                        "added_at": user.registered_at,
                        "verified_at": user.registered_at,
                    }
                    for source_id, target_id, user in valid
                ]
            )
            .on_conflict_do_nothing()
        )

    graph = [(source_id, target_id, users[source_id]) for source_id, target_id in resolved.items()]
    await session.execute(
        insert(UserProfile)
        .values(
            [
                {
                    "account_id": target_id,
                    "bio": user.bio,
                    "social_links": {},
                    "default_ruleset": user.default_ruleset,
                    "play_style": user.play_style,
                }
                for _, target_id, user in graph
            ]
        )
        .on_conflict_do_nothing(index_elements=(UserProfile.account_id,))
    )
    preference_statement = insert(UserPreference).values(
        [
            {
                "account_id": target_id,
                "locale": "en",
                "timezone": "UTC",
                "theme": "system",
                "master_volume": 1.0,
                "music_volume": 1.0,
                "effect_volume": 1.0,
                "preferred_ranking_policy": f"stable.{user.default_ruleset.value}.ranked",
                "private_message_policy": "friends",
                "invisible_online": False,
                "profile_section_order": [],
                "extra": user.metadata,
                "created_at": user.registered_at,
                "updated_at": user.registered_at,
            }
            for _, target_id, user in graph
        ]
    )
    await session.execute(
        preference_statement.on_conflict_do_update(
            index_elements=(UserPreference.account_id,),
            set_={
                "extra": preference_statement.excluded.extra.op("||")(UserPreference.extra),
                "updated_at": UserPreference.updated_at,
            },
        )
    )

    credentials = [
        {
            "account_id": target_id,
            "verifier": user.password_verifier,
            "algorithm": "bcrypt_md5",
            "pepper_version": None,
            "password_changed_at": user.registered_at,
            "must_change": False,
            "created_at": user.registered_at,
            "updated_at": user.registered_at,
        }
        for _, target_id, user in graph
        if user.password_verifier is not None
    ]
    if credentials:
        await session.execute(
            insert(PasswordCredential)
            .values(credentials)
            .on_conflict_do_nothing(index_elements=(PasswordCredential.account_id,))
        )


async def _insert_access_facts(
    session: AsyncSession,
    runtime: MigrationRuntime,
    resolved: dict[int, int],
    users: dict[int, _PreparedUser],
) -> None:
    roles: dict[str, int] = dict(
        (
            await session.execute(
                select(Role.code, Role.id).where(Role.code.in_(("user", "moderator", "administrator")))
            )
        ).all()
    )
    permissions: dict[str, int] = dict(
        (
            await session.execute(
                select(Permission.code, Permission.id).where(
                    Permission.code.in_(("admin.access", "content.manage", "multiplayer.manage"))
                )
            )
        ).all()
    )
    entitlement_id = await session.scalar(select(Entitlement.id).where(Entitlement.code == "supporter"))
    if (
        set(roles) != {"user", "moderator", "administrator"}
        or set(permissions)
        != {
            "admin.access",
            "content.manage",
            "multiplayer.manage",
        }
        or entitlement_id is None
    ):
        raise RuntimeError("bootstrap authorization catalog is incomplete")

    account_ids = set(resolved.values())
    account_types: dict[int, AccountType] = dict(
        (await session.execute(select(Account.id, Account.type).where(Account.id.in_(account_ids)))).all()
    )
    active_roles: set[tuple[int, int]] = {
        (account_id, role_id)
        for account_id, role_id in (
            await session.execute(
                select(AccountRoleGrant.account_id, AccountRoleGrant.role_id).where(
                    AccountRoleGrant.account_id.in_(account_ids),
                    AccountRoleGrant.revoked_at.is_(None),
                    AccountRoleGrant.starts_at <= runtime.started_at,
                    or_(AccountRoleGrant.ends_at.is_(None), AccountRoleGrant.ends_at > runtime.started_at),
                )
            )
        ).all()
    }
    active_permissions: set[tuple[int, int]] = {
        (account_id, permission_id)
        for account_id, permission_id in (
            await session.execute(
                select(AccountPermissionGrant.account_id, AccountPermissionGrant.permission_id).where(
                    AccountPermissionGrant.account_id.in_(account_ids),
                    AccountPermissionGrant.effect == GrantEffect.ALLOW,
                    AccountPermissionGrant.revoked_at.is_(None),
                    AccountPermissionGrant.starts_at <= runtime.started_at,
                    or_(AccountPermissionGrant.ends_at.is_(None), AccountPermissionGrant.ends_at > runtime.started_at),
                )
            )
        ).all()
    }
    active_entitlements = set(
        (
            await session.scalars(
                select(AccountEntitlementGrant.account_id).where(
                    AccountEntitlementGrant.account_id.in_(account_ids),
                    AccountEntitlementGrant.entitlement_id == entitlement_id,
                    AccountEntitlementGrant.revoked_at.is_(None),
                    AccountEntitlementGrant.starts_at <= runtime.started_at,
                    or_(
                        AccountEntitlementGrant.ends_at.is_(None),
                        AccountEntitlementGrant.ends_at > runtime.started_at,
                    ),
                )
            )
        ).all()
    )

    role_rows: list[dict[str, object]] = []
    permission_rows: list[dict[str, object]] = []
    entitlement_rows: list[dict[str, object]] = []
    for source_id, account_id in resolved.items():
        user = users[source_id]
        role_codes = set() if account_types.get(account_id) is AccountType.BOT else {"user"}
        if user.privilege_mask & _MODERATOR:
            role_codes.add("moderator")
        if user.privilege_mask & (_ADMINISTRATOR | _DEVELOPER):
            role_codes.add("administrator")
        for code in role_codes:
            role_id = roles[code]
            if (account_id, role_id) not in active_roles:
                role_rows.append(
                    {
                        "id": runtime.ids.make("account-role-grant", f"{source_id}:{code}"),
                        "account_id": account_id,
                        "role_id": role_id,
                        "starts_at": user.registered_at,
                        "reason": "Imported from bancho.py privileges.",
                        "created_at": user.registered_at,
                    }
                )
                active_roles.add((account_id, role_id))

        direct_codes: set[str] = set()
        if user.privilege_mask & _TOURNEY_MANAGER:
            direct_codes.add("multiplayer.manage")
        if user.privilege_mask & _NOMINATOR:
            direct_codes.add("content.manage")
        if user.privilege_mask & _ADMINISTRATOR:
            direct_codes.add("admin.access")
        for code in direct_codes:
            permission_id = permissions[code]
            if (account_id, permission_id) not in active_permissions:
                permission_rows.append(
                    {
                        "id": runtime.ids.make("account-permission-grant", f"{source_id}:{code}"),
                        "account_id": account_id,
                        "permission_id": permission_id,
                        "effect": GrantEffect.ALLOW,
                        "starts_at": user.registered_at,
                        "reason": "Imported from bancho.py privileges.",
                        "created_at": user.registered_at,
                    }
                )
                active_permissions.add((account_id, permission_id))

        if (
            account_id not in active_entitlements
            and user.privilege_mask & _DONATOR
            and (user.supporter_ends_at is None or user.supporter_ends_at > user.registered_at)
        ):
            entitlement_rows.append(
                {
                    "id": runtime.ids.make("account-entitlement-grant", f"{source_id}:supporter"),
                    "account_id": account_id,
                    "entitlement_id": entitlement_id,
                    "starts_at": user.registered_at,
                    "ends_at": user.supporter_ends_at,
                    "source": "migration",
                    "reference_id": f"user:{source_id}",
                    "created_at": user.registered_at,
                }
            )
            active_entitlements.add(account_id)

    if role_rows:
        await session.execute(insert(AccountRoleGrant).values(role_rows).on_conflict_do_nothing())
    if permission_rows:
        await session.execute(insert(AccountPermissionGrant).values(permission_rows).on_conflict_do_nothing())
    if entitlement_rows:
        await session.execute(insert(AccountEntitlementGrant).values(entitlement_rows).on_conflict_do_nothing())


async def _insert_moderation_facts(
    session: AsyncSession,
    runtime: MigrationRuntime,
    resolved: dict[int, int],
    users: dict[int, _PreparedUser],
) -> None:
    account_ids = set(resolved.values())
    active_sanctions: set[tuple[int, SanctionKind]] = {
        (account_id, kind)
        for account_id, kind in (
            await session.execute(
                select(Sanction.subject_account_id, Sanction.kind).where(
                    Sanction.subject_account_id.in_(account_ids),
                    Sanction.revoked_at.is_(None),
                    or_(Sanction.ends_at.is_(None), Sanction.ends_at > runtime.started_at),
                )
            )
        ).all()
    }
    case_rows: list[dict[str, object]] = []
    sanction_rows: list[dict[str, object]] = []
    event_rows: list[dict[str, object]] = []
    for source_id, account_id in resolved.items():
        user = users[source_id]
        sanctions: list[tuple[str, SanctionKind, datetime | None, str]] = []
        if not user.privilege_mask & _UNRESTRICTED:
            sanctions.append(("restriction", SanctionKind.RESTRICTION, None, "Active legacy account restriction."))
        if user.silence_ends_at is not None and user.silence_ends_at > runtime.started_at:
            sanctions.append(("silence", SanctionKind.SILENCE, user.silence_ends_at, "Active legacy silence."))
        for label, kind, ends_at, reason in sanctions:
            if (account_id, kind) in active_sanctions:
                runtime.report.increment("identity.users", "sanction_target_wins")
                continue
            case_id = runtime.ids.make("moderation-case", f"user:{source_id}:{label}")
            sanction_id = runtime.ids.make("sanction", f"user:{source_id}:{label}")
            case_rows.append(
                {
                    "id": case_id,
                    "subject_account_id": account_id,
                    "status": "open",
                    "summary": reason,
                    "severity": 50 if kind is SanctionKind.RESTRICTION else 20,
                    "created_at": runtime.started_at,
                }
            )
            sanction_rows.append(
                {
                    "id": sanction_id,
                    "case_id": case_id,
                    "subject_account_id": account_id,
                    "kind": kind,
                    "starts_at": runtime.started_at,
                    "ends_at": ends_at,
                    "reason": reason,
                    "created_at": runtime.started_at,
                }
            )
            event_rows.append(
                {
                    "id": _negative_id(runtime, "sanction-event", f"{source_id}:{label}"),
                    "sanction_id": sanction_id,
                    "action": "imposed",
                    "reason": reason,
                    "source_id": source_id,
                    "created_at": runtime.started_at,
                }
            )
            active_sanctions.add((account_id, kind))
    if case_rows:
        await session.execute(insert(ModerationCase).values(case_rows).on_conflict_do_nothing())
        await session.execute(insert(Sanction).values(sanction_rows).on_conflict_do_nothing())
        statement = text(
            """
            INSERT INTO moderation.sanction_event
                (id, sanction_id, actor_account_id, action, reason, details, created_at)
            OVERRIDING SYSTEM VALUE
            VALUES
                (:id, :sanction_id, NULL, :action, :reason,
                 jsonb_build_object('migration_id', :migration_id, 'source_user_id', :source_id), :created_at)
            ON CONFLICT DO NOTHING
            """
        )
        for row in event_rows:
            await session.execute(statement, {**row, "migration_id": runtime.config.migration_id})


async def _migrate_client_hashes(
    session: AsyncSession,
    runtime: MigrationRuntime,
    resolved: dict[int, int],
) -> None:
    source_ids = sorted(resolved)
    placeholders = ", ".join(["%s"] * len(source_ids))
    rows = runtime.source.fetch_all(
        "client_hashes",
        columns=("userid", "osupath", "adapters", "uninstall_id", "disk_serial", "latest_time", "occurrences"),
        order_by=("userid", "osupath", "adapters", "uninstall_id", "disk_serial"),
        where=f"`userid` IN ({placeholders})",
        parameters=source_ids,
    )
    key = settings.device_hmac_key.get_secret_value().encode()
    prepared: list[_PreparedDevice] = []
    for row in rows:
        try:
            source_id = _positive_int(row.get("userid"), "client hash user id")
            account_id = resolved[source_id]
            components = tuple(
                sorted(
                    (kind, digest_device_component(value, key=key))
                    for kind, column in _DEVICE_COMPONENTS
                    if (value := _device_value(row.get(column))) is not None
                )
            )
            if not components:
                raise ValueError("client hash has no usable components")
            material = b"".join(
                len(kind.encode()).to_bytes(2, "big") + kind.encode() + digest for kind, digest in components
            )
            fingerprint = digest_device_component(material, key=key)
            raw_occurrences = row.get("occurrences")
            if isinstance(raw_occurrences, bool) or not isinstance(raw_occurrences, int) or raw_occurrences < 0:
                raise ValueError("client hash occurrences must be a nonnegative integer")
            occurrences = max(1, raw_occurrences)
            prepared.append(
                _PreparedDevice(
                    account_id,
                    runtime.ids.make("device", fingerprint.hex()),
                    fingerprint,
                    components,
                    _source_timestamp(runtime, row.get("latest_time")),
                    occurrences,
                )
            )
        except (KeyError, ValueError) as error:
            runtime.report.increment("identity.users", "client_hash_skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "client_hash_malformed",
                str(error),
                entity="client_hashes",
                source_id=row.get("userid"),
            )
    if not prepared:
        return

    await session.execute(
        insert(Device)
        .values(
            [
                {
                    "id": item.proposed_id,
                    "fingerprint_hmac": item.fingerprint_hmac,
                    "platform": None,
                    "first_seen_at": item.seen_at,
                    "last_seen_at": item.seen_at,
                    "risk_level": 0,
                    "created_at": item.seen_at,
                    "updated_at": item.seen_at,
                }
                for item in prepared
            ]
        )
        .on_conflict_do_nothing()
    )
    device_ids: dict[bytes, uuid.UUID] = dict(
        (
            await session.execute(
                select(Device.fingerprint_hmac, Device.id).where(
                    Device.fingerprint_hmac.in_({item.fingerprint_hmac for item in prepared})
                )
            )
        ).all()
    )
    identifiers: list[dict[str, object]] = []
    account_devices: list[dict[str, object]] = []
    for item in prepared:
        device_id = device_ids.get(item.fingerprint_hmac)
        if device_id is None:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "device_identity_collision",
                "deterministic device ID collided with unrelated target data",
                entity="client_hashes",
            )
            continue
        identifiers.extend(
            {
                "device_id": device_id,
                "kind": kind,
                "value_hmac": digest,
                "quality": 0,
                "created_at": item.seen_at,
            }
            for kind, digest in item.components
        )
        account_devices.append(
            {
                "account_id": item.account_id,
                "device_id": device_id,
                "first_used_at": item.seen_at,
                "last_used_at": item.seen_at,
                "use_count": item.occurrences,
            }
        )
    if identifiers:
        await session.execute(insert(DeviceIdentifier).values(identifiers).on_conflict_do_nothing())
    if account_devices:
        await session.execute(insert(AccountDevice).values(account_devices).on_conflict_do_nothing())
    runtime.report.increment("identity.users", "client_hashes_imported", len(account_devices))


async def _migrate_auth_attempt_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    key = settings.device_hmac_key.get_secret_value().encode()
    statement = text(
        """
        INSERT INTO iam.auth_attempt
            (id, account_id, session_id, device_id, identifier_hmac, ip_address,
             client_family, client_version, result, failure_reason, country_code, asn, context, created_at)
        OVERRIDING SYSTEM VALUE
        VALUES
            (:id, :account_id, NULL, NULL, :identifier_hmac, CAST(:ip_address AS inet),
             :client_family, :client_version, :result, NULL, NULL, NULL, :context, :created_at)
        ON CONFLICT DO NOTHING
        """
    ).bindparams(bindparam("context", type_=JSONB))
    imported = 0
    for row in rows:
        try:
            source_login_id = _positive_int(row.get("id"), "login id")
            source_user_id = _positive_int(row.get("userid"), "login user id")
            account_id = runtime.mappings.accounts[source_user_id]
            ip_address = str(ipaddress.ip_address(str(row.get("ip"))))
            client_version = _client_version(row.get("osu_ver"), row.get("osu_stream"))
            created_at = _source_timestamp(runtime, row.get("datetime"))
        except (KeyError, ValueError) as error:
            runtime.report.increment("identity.auth_attempts", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "auth_attempt_malformed",
                str(error),
                entity="ingame_logins",
                source_id=row.get("id"),
            )
            continue
        await session.execute(
            statement,
            {
                "id": _negative_id(runtime, "auth-attempt", source_login_id),
                "account_id": account_id,
                "identifier_hmac": hmac_sha256_digest(f"id:{account_id}", key=key),
                "ip_address": ip_address,
                "client_family": ClientFamily.STABLE.value,
                "client_version": client_version,
                "result": "success",
                "context": {
                    "migration_id": runtime.config.migration_id,
                    "source_login_id": source_login_id,
                    "osu_stream": str(row.get("osu_stream") or ""),
                },
                "created_at": created_at,
            },
        )
        imported += 1
    runtime.report.increment("identity.auth_attempts", "imported", imported)


async def _migrate_audit_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    imported = 0
    for row in rows:
        try:
            source_id = _positive_int(row.get("id"), "log id")
            source_actor = _positive_int(row.get("from"), "log actor")
            source_target = _positive_int(row.get("to"), "log target")
            action = row.get("action")
            if not isinstance(action, str) or not action.strip() or len(action.strip()) > 32:
                raise ValueError("log action is invalid")
            message = row.get("msg")
            if message is not None and not isinstance(message, str):
                raise ValueError("log message must be text or null")
            actor_id = runtime.mappings.accounts.get(source_actor)
            target_id = runtime.mappings.accounts.get(source_target)
            if target_id is None:
                raise ValueError("log target account was not migrated")
            statement = insert(AuditEvent).values(
                actor_account_id=actor_id,
                action=f"legacy.{action.strip().casefold()}",
                target_type="account",
                target_id=str(target_id),
                reason=message,
                metadata_json={
                    "migration_id": runtime.config.migration_id,
                    "source_log_id": source_id,
                    "source_actor_id": source_actor,
                    "source_target_id": source_target,
                },
                created_at=_source_timestamp(runtime, row.get("time")),
            )
            await session.execute(statement)
            imported += 1
        except (KeyError, ValueError) as error:
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "audit_log_skipped",
                str(error),
                entity="logs",
                source_id=row.get("id"),
            )
            runtime.report.increment("identity.audit_logs", "skipped")
    runtime.report.increment("identity.audit_logs", "imported", imported)


def _source_timestamp(runtime: MigrationRuntime, value: object) -> datetime:
    if isinstance(value, datetime):
        return aware_datetime(value, runtime.config.source_timezone, fallback=runtime.started_at)
    return unix_datetime(value, fallback=runtime.started_at)


def _optional_unix_timestamp(runtime: MigrationRuntime, value: object) -> datetime | None:
    if value in {None, 0, "", "0"}:
        return None
    return unix_datetime(value, fallback=runtime.started_at)


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _country_code(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    country = value.strip().upper()
    return country if len(country) == 2 and country.isalpha() and country.isascii() else None


def _password_verifier(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str) or len(value) != 60 or not value.startswith(("$2a$", "$2b$", "$2y$")):
        return None
    return value


def _device_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _client_version(version: object, stream: object) -> str | None:
    if isinstance(version, datetime):
        version_text = version.date().strftime("b%Y%m%d")
    elif isinstance(version, date):
        version_text = version.strftime("b%Y%m%d")
    elif version is None:
        return None
    else:
        version_text = str(version).strip()
    stream_text = str(stream).strip() if stream is not None else ""
    combined = f"{version_text}{stream_text}"
    return combined[:64] or None


def _negative_id(runtime: MigrationRuntime, entity: str, source_id: object) -> int:
    return -((runtime.ids.make(entity, source_id).int % ((1 << 63) - 1)) + 1)


async def _next_account_id(session: AsyncSession, *, reserved: set[int]) -> int:
    target_max = await session.scalar(text("SELECT COALESCE(MAX(id), 0) FROM core.account"))
    if not isinstance(target_max, int):
        raise RuntimeError("target account maximum is not an integer")
    reserved_max = max((value for value in reserved if value <= 2_147_483_647), default=0)
    next_id = max(target_max, reserved_max) + 1
    while next_id in reserved:
        next_id += 1
    if not 1 <= next_id <= 2_147_483_647:
        raise RuntimeError("no Stable-compatible target account ID is available")
    return next_id


def _account_error(
    runtime: MigrationRuntime,
    code: str,
    message: str,
    source_id: int,
    details: dict[str, object] | None = None,
) -> None:
    runtime.report.increment("identity.users", "skipped_ambiguous")
    runtime.report.add(
        DiagnosticSeverity.ERROR,
        code,
        message,
        entity="users",
        source_id=source_id,
        details=details,
    )
