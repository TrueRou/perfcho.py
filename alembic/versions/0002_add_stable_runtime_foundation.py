"""Add Stable runtime integrity constraints and deterministic bootstrap data."""

import sqlalchemy as sa

from alembic import op

revision = "0002_stable_runtime"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add Stable storage links, cross-table integrity, and runtime seeds."""
    op.create_check_constraint(
        "ck_accounts_stable_id_range",
        "accounts",
        "id BETWEEN 1 AND 2147483647",
        schema="core",
    )
    op.add_column("beatmap_revisions", sa.Column("file_asset_id", sa.Uuid(), nullable=True), schema="content")
    op.create_foreign_key(
        "fk_beatmap_revisions_file_asset_id_media_assets",
        "beatmap_revisions",
        "media_assets",
        ["file_asset_id"],
        ["id"],
        source_schema="content",
        referent_schema="core",
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_beatmap_revisions_file_asset_id",
        "beatmap_revisions",
        ["file_asset_id"],
        schema="content",
    )

    op.create_unique_constraint(
        "uq_auth_sessions_id_account",
        "auth_sessions",
        ["id", "account_id"],
        schema="iam",
    )
    op.drop_constraint("fk_auth_tokens_session_id_auth_sessions", "auth_tokens", schema="iam", type_="foreignkey")
    op.create_foreign_key(
        "fk_auth_tokens_session_account",
        "auth_tokens",
        "auth_sessions",
        ["session_id", "account_id"],
        ["id", "account_id"],
        source_schema="iam",
        referent_schema="iam",
        ondelete="CASCADE",
    )

    op.create_check_constraint(
        "ck_rounds_single_source",
        "rounds",
        "num_nonnulls(playlist_revision_id, tournament_pool_item_id) = 1",
        schema="multiplayer",
    )
    op.create_index(
        "uq_calculation_releases_active_kind_ruleset",
        "calculation_releases",
        ["kind", "ruleset"],
        unique=True,
        schema="scoring",
        postgresql_where=sa.text("active"),
    )
    op.create_index(
        "uq_ranking_policies_active_scoreboard",
        "ranking_policies",
        ["scoreboard_id"],
        unique=True,
        schema="scoring",
        postgresql_where=sa.text("active"),
    )

    op.execute(
        sa.text(
            "INSERT INTO authz.permissions (id, code, description) VALUES "
            "(1, 'bancho.login', 'Log in through the Stable Bancho protocol'), "
            "(2, 'chat.read', 'Read public and joined chat channels'), "
            "(3, 'chat.write', 'Send public and direct chat messages'), "
            "(4, 'multiplayer.play', 'Create and join multiplayer matches'), "
            "(5, 'multiplayer.tourney', 'Use official tournament observer packets'), "
            "(6, 'moderation.manage', 'Manage sanctions and moderation cases')"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO authz.roles (id, code, name, description, priority, system) VALUES "
            "(1, 'user', 'User', 'Default capabilities for active accounts', 1000, true), "
            "(2, 'tournament', 'Tournament Staff', 'Tournament observer capabilities', 500, true), "
            "(3, 'moderator', 'Moderator', 'Community moderation capabilities', 100, true)"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO authz.role_permissions (role_id, permission_id) VALUES "
            "(1, 1), (1, 2), (1, 3), (1, 4), "
            "(2, 1), (2, 2), (2, 3), (2, 4), (2, 5), "
            "(3, 1), (3, 2), (3, 3), (3, 4), (3, 6)"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO authz.entitlements (id, code, name, description) VALUES "
            "(1, 'supporter', 'Supporter', 'Time-bounded supporter client features')"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO community.channels "
            "(id, kind, slug, name, description, read_permission_id, write_permission_id, auto_join) "
            "OVERRIDING SYSTEM VALUE VALUES "
            "(1, 'public', 'osu', '#osu', 'General discussion', 2, 3, true), "
            "(2, 'public', 'announce', '#announce', 'Server announcements', 2, NULL, true), "
            "(3, 'public', 'lobby', '#lobby', 'Multiplayer lobby', 4, 4, false)"
        )
    )
    op.execute(sa.text("SELECT setval(pg_get_serial_sequence('community.channels', 'id'), 3, true)"))

    op.execute(
        sa.text(
            "INSERT INTO scoring.mod_policies "
            "(id, name, schema_version, rules, digest) VALUES "
            "('00000000-0000-7000-8000-000000000201', 'Stable ranked mods', 1, "
            "'{\"allowed_legacy_mask\": 2147483647}'::jsonb, "
            "decode('f45f91b50d56a29427e27b4ce59ebce862b691d0d51e8780ccc2635baca257ae', 'hex'))"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO scoring.mod_sets "
            "(id, scoreboard_id, canonical, canonical_digest, legacy_bits) OVERRIDING SYSTEM VALUE "
            "SELECT scoreboard_id, scoreboard_id, '[]'::jsonb, "
            "decode('4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945', 'hex'), 0 "
            "FROM generate_series(1, 8) AS scoreboard_id ORDER BY scoreboard_id"
        )
    )
    op.execute(sa.text("SELECT setval(pg_get_serial_sequence('scoring.mod_sets', 'id'), 8, true)"))

    _seed_calculation_releases()
    _seed_ranking_policies()


def downgrade() -> None:
    """Remove Stable runtime seeds and restore the initial schema contract."""
    op.execute(sa.text("DELETE FROM scoring.ranking_policies WHERE code LIKE 'stable_%'"))
    op.execute(sa.text("DELETE FROM scoring.calculation_releases WHERE engine = 'rosu-pp' AND version = 'current'"))
    op.execute(
        sa.text(
            "DELETE FROM scoring.mod_sets WHERE legacy_bits = 0 AND canonical = '[]'::jsonb "
            "AND canonical_digest = "
            "decode('4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945', 'hex')"
        )
    )
    op.execute(sa.text("DELETE FROM scoring.mod_policies WHERE id = '00000000-0000-7000-8000-000000000201'"))
    op.execute(sa.text("DELETE FROM community.channels WHERE id BETWEEN 1 AND 3"))
    op.execute(sa.text("DELETE FROM authz.entitlements WHERE id = 1"))
    op.execute(sa.text("DELETE FROM authz.role_permissions WHERE role_id BETWEEN 1 AND 3"))
    op.execute(sa.text("DELETE FROM authz.roles WHERE id BETWEEN 1 AND 3"))
    op.execute(sa.text("DELETE FROM authz.permissions WHERE id BETWEEN 1 AND 6"))

    op.drop_index("uq_ranking_policies_active_scoreboard", table_name="ranking_policies", schema="scoring")
    op.drop_index(
        "uq_calculation_releases_active_kind_ruleset",
        table_name="calculation_releases",
        schema="scoring",
    )
    op.drop_constraint("ck_rounds_single_source", "rounds", schema="multiplayer", type_="check")
    op.drop_constraint("fk_auth_tokens_session_account", "auth_tokens", schema="iam", type_="foreignkey")
    op.create_foreign_key(
        "fk_auth_tokens_session_id_auth_sessions",
        "auth_tokens",
        "auth_sessions",
        ["session_id"],
        ["id"],
        source_schema="iam",
        referent_schema="iam",
        ondelete="CASCADE",
    )
    op.drop_constraint("uq_auth_sessions_id_account", "auth_sessions", schema="iam", type_="unique")
    op.drop_constraint(
        "uq_beatmap_revisions_file_asset_id",
        "beatmap_revisions",
        schema="content",
        type_="unique",
    )
    op.drop_constraint(
        "fk_beatmap_revisions_file_asset_id_media_assets",
        "beatmap_revisions",
        schema="content",
        type_="foreignkey",
    )
    op.drop_column("beatmap_revisions", "file_asset_id", schema="content")
    op.drop_constraint("ck_accounts_stable_id_range", "accounts", schema="core", type_="check")


def _seed_calculation_releases() -> None:
    """Seed one active reproducible performance release per ruleset."""
    op.execute(
        sa.text(
            "INSERT INTO scoring.calculation_releases "
            "(id, kind, ruleset, engine, version, artifact_digest, configuration, active) VALUES "
            "('00000000-0000-7000-8000-000000000301', 'performance', 'osu', 'rosu-pp', 'current', "
            "decode(repeat('01', 32), 'hex'), '{}'::jsonb, true), "
            "('00000000-0000-7000-8000-000000000302', 'performance', 'taiko', 'rosu-pp', 'current', "
            "decode(repeat('02', 32), 'hex'), '{}'::jsonb, true), "
            "('00000000-0000-7000-8000-000000000303', 'performance', 'fruits', 'rosu-pp', 'current', "
            "decode(repeat('03', 32), 'hex'), '{}'::jsonb, true), "
            "('00000000-0000-7000-8000-000000000304', 'performance', 'mania', 'rosu-pp', 'current', "
            "decode(repeat('04', 32), 'hex'), '{}'::jsonb, true)"
        )
    )


def _seed_ranking_policies() -> None:
    """Seed one active Stable score ranking policy per scoreboard."""
    values = []
    board_configuration = {
        1: ("osu", 1),
        2: ("taiko", 2),
        3: ("fruits", 3),
        4: ("mania", 4),
        5: ("osu_relax", 1),
        6: ("taiko_relax", 2),
        7: ("fruits_relax", 3),
        8: ("osu_autopilot", 1),
    }
    for scoreboard_id, (board_code, release_suffix) in board_configuration.items():
        values.append(
            "("
            f"'00000000-0000-7000-8000-{400 + scoreboard_id:012d}', "
            f"'stable_{board_code}', 1, {scoreboard_id}, 'classic_score', 'score_id', "
            "'00000000-0000-7000-8000-000000000201', "
            f"'00000000-0000-7000-8000-{300 + release_suffix:012d}', "
            "'{}'::jsonb, true)"
        )
    op.execute(
        sa.text(
            "INSERT INTO scoring.ranking_policies "
            "(id, code, version, scoreboard_id, metric, tie_breaker, mod_policy_id, "
            "calculation_release_id, configuration, active) VALUES " + ", ".join(values)
        )
    )
