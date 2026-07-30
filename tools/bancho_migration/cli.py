"""Expose the bancho.py migration as an explicit maintenance command."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

from tools.bancho_migration.config import MigrationConfig
from tools.bancho_migration.runner import run_migration, run_preflight, run_verify


def build_parser() -> argparse.ArgumentParser:
    """Build the command parser without reading environment configuration."""
    parser = argparse.ArgumentParser(description="Migrate bancho.py v5.2.2 data into perfcho")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("preflight", "apply", "verify"):
        child = subparsers.add_parser(command)
        child.add_argument("--source-url", help="MySQL URL; defaults to BANCHO_DATABASE_URL")
        child.add_argument("--target-url", help="PostgreSQL asyncpg URL; defaults to DATABASE_URL")
        child.add_argument("--data-dir", type=Path, required=True, help="bancho root or its .data directory")
        child.add_argument("--migration-id", required=True)
        child.add_argument("--source-timezone", default="UTC", help="IANA timezone used by legacy DATETIME columns")
        child.add_argument("--batch-size", type=int, default=1000)
        child.add_argument("--overrides", type=Path)
        child.add_argument("--report", type=Path, default=Path("bancho-migration-report.json"))
        if command == "apply":
            child.add_argument("--confirm-offline", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one migration command and return a shell-compatible status code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = MigrationConfig.from_values(
            source_url=args.source_url,
            target_url=args.target_url,
            data_directory=args.data_dir,
            migration_id=args.migration_id,
            source_timezone=args.source_timezone,
            batch_size=args.batch_size,
            report_path=args.report,
            overrides_path=args.overrides,
            confirm_offline=getattr(args, "confirm_offline", False),
        )
        operation = {
            "preflight": run_preflight,
            "apply": run_migration,
            "verify": run_verify,
        }[args.command]
        report = asyncio.run(operation(config))
    except Exception as error:
        parser.exit(1, f"bancho migration failed: {error}\n")
    print(f"report: {config.report_path}")
    print(f"diagnostics: {len(report.diagnostics)}, errors: {report.has_errors}")
    return 1 if report.has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
