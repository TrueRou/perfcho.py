from pathlib import Path

from sqlalchemy import Enum
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import configure_mappers
from sqlalchemy.schema import CreateIndex, CreateTable

from perfcho.infra.database import MODEL_SCHEMAS, DbBase, models

EXPECTED_TABLES = {
    "audit": {"audit_events"},
    "authz": {
        "account_entitlement_grants",
        "account_permission_grants",
        "account_role_grants",
        "entitlements",
        "permissions",
        "role_permissions",
        "roles",
    },
    "community": {
        "channel_memberships",
        "channel_user_states",
        "channels",
        "direct_conversations",
        "message_revisions",
        "messages",
        "notification_dispatches",
        "notification_preferences",
        "notification_recipients",
        "notifications",
    },
    "content": {
        "beatmap_owners",
        "beatmap_revisions",
        "beatmap_status_events",
        "beatmap_tag_votes",
        "beatmaps",
        "beatmapset_assets",
        "beatmapset_favourites",
        "beatmapsets",
        "comments",
        "map_status_requests",
        "rating_votes",
        "sources",
        "sync_states",
        "tag_definitions",
    },
    "core": {
        "account_badges",
        "account_emails",
        "account_names",
        "accounts",
        "badges",
        "media_assets",
        "user_preferences",
        "user_profiles",
    },
    "events": {"activity_events", "outbox_deliveries", "outbox_events", "projection_checkpoints"},
    "iam": {
        "account_devices",
        "auth_attempts",
        "auth_challenges",
        "auth_sessions",
        "auth_token_scopes",
        "auth_tokens",
        "device_identifiers",
        "devices",
        "oauth_client_redirect_uris",
        "oauth_client_scopes",
        "oauth_client_secrets",
        "oauth_clients",
        "password_credentials",
        "recovery_codes",
        "scopes",
        "totp_factors",
    },
    "moderation": {
        "anticheat_detectors",
        "anticheat_findings",
        "anticheat_runs",
        "case_entries",
        "case_findings",
        "cases",
        "sanction_events",
        "sanctions",
    },
    "multiplayer": {
        "attempts",
        "daily_challenge_completions",
        "daily_challenge_user_summaries",
        "daily_challenges",
        "events",
        "matchmaking_assignment_members",
        "matchmaking_assignments",
        "matchmaking_group_members",
        "matchmaking_groups",
        "matchmaking_queues",
        "matchmaking_rating_changes",
        "matchmaking_ratings",
        "matchmaking_tickets",
        "playlist_item_user_summaries",
        "playlist_items",
        "playlist_revisions",
        "room_participants",
        "room_user_summaries",
        "rooms",
        "round_participants",
        "round_results",
        "rounds",
        "session_pool_bindings",
        "session_presences",
        "session_standings",
        "sessions",
        "tournament_pool_items",
        "tournament_pool_revisions",
        "tournament_pools",
    },
    "scoring": {
        "beatmap_activity",
        "beatmap_difficulty_attributes",
        "beatmap_fail_histograms",
        "calculation_releases",
        "leaderboard_entries",
        "mod_policies",
        "mod_sets",
        "play_attempts",
        "rank_snapshots",
        "ranking_policies",
        "replay_view_events",
        "replays",
        "score_attestations",
        "score_eligibility",
        "score_hit_statistics",
        "score_performances",
        "scoreboards",
        "scores",
        "user_beatmap_activity",
        "user_monthly_activity",
        "user_play_stats",
        "user_ranked_stats",
    },
    "social": {
        "achievement_definitions",
        "achievement_translations",
        "achievement_unlocks",
        "blocks",
        "follows",
        "team_join_requests",
        "team_memberships",
        "teams",
    },
    "system": {"maintenance_states", "server_settings"},
}


def test_model_registry_is_complete() -> None:
    assert models
    configure_mappers()

    for mapper in DbBase.registry.mappers:
        docstring = mapper.class_.__doc__
        assert docstring
        assert docstring.isascii()

    actual: dict[str, set[str]] = {schema: set() for schema in MODEL_SCHEMAS}
    for table in DbBase.metadata.tables.values():
        assert table.schema is not None
        assert table.comment is None
        actual[table.schema].add(table.name)
        assert table.primary_key.columns
        for foreign_key in table.foreign_keys:
            assert foreign_key.column.table.key in DbBase.metadata.tables

    assert actual == EXPECTED_TABLES
    assert len(DbBase.metadata.tables) == 129
    assert "public" not in actual


def test_postgresql_ddl_compiles() -> None:
    dialect = postgresql.dialect()
    for table in DbBase.metadata.sorted_tables:
        assert str(CreateTable(table).compile(dialect=dialect))
        for index in table.indexes:
            assert str(CreateIndex(index).compile(dialect=dialect))


def test_database_object_names_are_unambiguous() -> None:
    indexes_by_schema: dict[str, list[str]] = {schema: [] for schema in MODEL_SCHEMAS}
    for table in DbBase.metadata.tables.values():
        constraint_names = [constraint.name for constraint in table.constraints]
        assert None not in constraint_names
        assert len(constraint_names) == len(set(constraint_names))
        indexes_by_schema[table.schema].extend(index.name for index in table.indexes if index.name is not None)

    for index_names in indexes_by_schema.values():
        assert len(index_names) == len(set(index_names))


def test_state_enums_are_check_constraints() -> None:
    enum_columns = [
        column for table in DbBase.metadata.tables.values() for column in table.columns if isinstance(column.type, Enum)
    ]
    assert enum_columns
    assert all(not column.type.native_enum and column.type.create_constraint for column in enum_columns)


def test_initial_migration_is_static_and_complete() -> None:
    migration = Path("alembic/versions/0001_initial_schema.py").read_text(encoding="utf-8")
    assert migration.count("op.create_table(") == len(DbBase.metadata.tables)
    assert "DbBase" not in migration
    assert "metadata.create_all" not in migration
    assert "metadata.drop_all" not in migration
    assert "create_constraint=True" not in migration
    assert "sa.sa." not in migration
    for schema in MODEL_SCHEMAS:
        assert f"CREATE SCHEMA {schema}" in migration
    for table in DbBase.metadata.tables.values():
        for index in table.indexes:
            assert index.name is not None
            assert f'"{index.name}"' in migration


def test_example_schema_was_removed() -> None:
    table_names = {table.name for table in DbBase.metadata.tables.values()}
    assert "tbl_user" not in table_names
    assert "tbl_note" not in table_names
    assert "notes" not in table_names
    assert "users" not in table_names
