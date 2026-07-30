"""Describe and validate the supported bancho.py v5.2.2 schema."""

from __future__ import annotations

from collections.abc import Mapping, Set

from tools.bancho_migration.models import DiagnosticSeverity, SourceSchema
from tools.bancho_migration.report import MigrationReport

EXCLUDED_TABLES = frozenset({"sb_patcher_scores_meta", "scores_suspicion", "performance_reports"})

REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "users": frozenset(
        {
            "id",
            "name",
            "safe_name",
            "email",
            "priv",
            "pw_bcrypt",
            "country",
            "silence_end",
            "donor_end",
            "creation_time",
            "latest_activity",
            "clan_id",
            "clan_priv",
            "preferred_mode",
            "play_style",
            "custom_badge_name",
            "custom_badge_icon",
            "userpage_content",
            "api_key",
        }
    ),
    "client_hashes": frozenset(
        {"userid", "osupath", "adapters", "uninstall_id", "disk_serial", "latest_time", "occurrences"}
    ),
    "ingame_logins": frozenset({"id", "userid", "ip", "osu_ver", "osu_stream", "datetime"}),
    "relationships": frozenset({"user1", "user2", "type"}),
    "clans": frozenset({"id", "name", "tag", "owner", "created_at"}),
    "channels": frozenset({"id", "name", "topic", "read_priv", "write_priv", "auto_join"}),
    "mail": frozenset({"id", "from_id", "to_id", "msg", "time", "read"}),
    "mapsets": frozenset({"server", "id", "last_osuapi_check"}),
    "maps": frozenset(
        {
            "server",
            "id",
            "set_id",
            "status",
            "md5",
            "artist",
            "title",
            "version",
            "creator",
            "filename",
            "last_update",
            "total_length",
            "max_combo",
            "frozen",
            "plays",
            "passes",
            "mode",
            "bpm",
            "cs",
            "ar",
            "od",
            "hp",
            "diff",
        }
    ),
    "map_requests": frozenset({"id", "map_id", "player_id", "datetime", "active"}),
    "favourites": frozenset({"userid", "setid", "created_at"}),
    "ratings": frozenset({"userid", "map_md5", "rating"}),
    "comments": frozenset({"id", "target_id", "target_type", "userid", "time", "comment", "colour"}),
    "scores": frozenset(
        {
            "id",
            "map_md5",
            "score",
            "pp",
            "acc",
            "max_combo",
            "mods",
            "n300",
            "n100",
            "n50",
            "nmiss",
            "ngeki",
            "nkatu",
            "grade",
            "status",
            "mode",
            "play_time",
            "time_elapsed",
            "client_flags",
            "userid",
            "perfect",
            "online_checksum",
        }
    ),
    "stats": frozenset(
        {
            "id",
            "mode",
            "tscore",
            "rscore",
            "pp",
            "plays",
            "playtime",
            "acc",
            "max_combo",
            "total_hits",
            "replay_views",
            "xh_count",
            "x_count",
            "sh_count",
            "s_count",
            "a_count",
        }
    ),
    "achievements": frozenset({"id", "file", "name", "desc", "cond"}),
    "user_achievements": frozenset({"userid", "achid"}),
    "logs": frozenset({"id", "from", "to", "action", "msg", "time"}),
    "tourney_pools": frozenset({"id", "name", "created_at", "created_by"}),
    "tourney_pool_maps": frozenset({"map_id", "pool_id", "mods", "slot"}),
    "startups": frozenset({"id", "ver_major", "ver_minor", "ver_micro", "datetime"}),
}


def validate_source_schema(schema: SourceSchema, report: MigrationReport) -> None:
    """Record all missing required tables and columns before any target write."""
    for table, required in REQUIRED_COLUMNS.items():
        actual = schema.tables.get(table)
        if actual is None:
            report.add(DiagnosticSeverity.ERROR, "source_table_missing", f"required source table {table} is missing")
            continue
        missing = required - actual
        if missing:
            report.add(
                DiagnosticSeverity.ERROR,
                "source_columns_missing",
                f"source table {table} is missing required columns",
                entity=table,
                details={"columns": sorted(missing)},
            )
    for table in EXCLUDED_TABLES & schema.tables.keys():
        report.add(
            DiagnosticSeverity.INFO,
            "source_table_excluded",
            f"excluded SB or obsolete telemetry table {table} will not be read",
            entity=table,
        )


def source_table_names() -> Set[str]:
    """Return the supported source tables used for inspection and fingerprinting."""
    return REQUIRED_COLUMNS.keys()


def required_columns(table: str) -> Mapping[str, object] | frozenset[str]:
    """Return the known source columns for a table."""
    return REQUIRED_COLUMNS[table]
