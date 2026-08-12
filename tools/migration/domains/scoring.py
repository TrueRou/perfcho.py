"""Reject the legacy scoring migration for the incompatible v6 schema."""

from tools.migration.models import MigrationRuntime


async def migrate_scoring(runtime: MigrationRuntime) -> None:
    """Reject imports targeting the removed v5 scoring catalog."""
    del runtime
    raise RuntimeError("legacy scoring migration is disabled for the v6 scoring schema")
