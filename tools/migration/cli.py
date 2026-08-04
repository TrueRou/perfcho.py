"""Expose the bancho.py migration as an explicit maintenance command."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

from loguru import logger

from perfcho.infra.logging import duration_ms, init_logger, log_event
from tools.migration.config import MigrationConfig
from tools.migration.models import DiagnosticSeverity, MigrationStatus
from tools.migration.report import MigrationReport
from tools.migration.runner import run_migration, run_preflight, run_verify


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
    init_logger("migration")
    parser = build_parser()
    args = parser.parse_args(argv)
    invocation_id = str(uuid.uuid4())
    migration_id = _safe_migration_id(args.migration_id)
    command = str(args.command)
    started_ns = time.monotonic_ns()
    log_event(
        "INFO",
        "migration.command.started",
        invocation_id=invocation_id,
        migration_id=migration_id,
        command=command,
    )
    config: MigrationConfig | None = None
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
        report = asyncio.run(operation(config, invocation_id=invocation_id))
    except KeyboardInterrupt as error:
        report = _failure_report(error, migration_id, invocation_id, command, interrupted=True)
        _write_fallback_report(report, args.report)
        log_event(
            "WARNING",
            "migration.command.interrupted",
            exception=error,
            invocation_id=invocation_id,
            migration_id=migration_id,
            command=command,
            status=MigrationStatus.INTERRUPTED.value,
            status_code=130,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
        )
        _print_summary(args.report, report)
        print("bancho migration interrupted", file=sys.stderr)
        logger.complete()
        return 130
    except BaseException as error:
        report = _failure_report(error, migration_id, invocation_id, command)
        _write_fallback_report(report, args.report)
        log_event(
            "ERROR",
            "migration.command.failed",
            exception=error,
            invocation_id=invocation_id,
            migration_id=migration_id,
            command=command,
            status=MigrationStatus.FAILED.value,
            status_code=1,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
        )
        _print_summary(args.report, report)
        print("bancho migration failed; inspect the report and structured logs", file=sys.stderr)
        logger.complete()
        return 1

    status_code = 1 if report.has_errors else 0
    if status_code:
        log_event(
            "ERROR",
            "migration.command.failed",
            invocation_id=invocation_id,
            migration_id=migration_id,
            command=command,
            status=report.status.value,
            status_code=status_code,
            failure="reported_errors",
            duration_ms=duration_ms(started_ns),
        )
    else:
        log_event(
            "INFO",
            "migration.command.completed",
            invocation_id=invocation_id,
            migration_id=migration_id,
            command=command,
            status=report.status.value,
            status_code=status_code,
            duration_ms=duration_ms(started_ns),
        )
    assert config is not None
    _print_summary(config.report_path, report)
    logger.complete()
    return status_code


def _safe_migration_id(value: object) -> str:
    candidate = str(value)
    if not candidate or len(candidate) > 64:
        return "invalid"
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
    return candidate if all(character in allowed for character in candidate) else "invalid"


def _failure_report(
    error: BaseException,
    migration_id: str,
    invocation_id: str,
    command: str,
    *,
    interrupted: bool = False,
) -> MigrationReport:
    attached = getattr(error, "migration_report", None)
    if isinstance(attached, MigrationReport):
        return attached
    report = MigrationReport(migration_id, invocation_id=invocation_id)
    report.add(
        DiagnosticSeverity.ERROR,
        "migration_interrupted" if interrupted else "migration_fatal_exception",
        "migration command was interrupted before completion"
        if interrupted
        else "migration command stopped because an unexpected exception was raised",
        details={"command": command, "error_type": type(error).__name__},
    )
    report.finish(MigrationStatus.INTERRUPTED if interrupted else MigrationStatus.FAILED)
    return report


def _write_fallback_report(report: MigrationReport, report_path: Path) -> None:
    if report.completed_at is None:
        report.finish(MigrationStatus.FAILED)
    try:
        report.write(report_path)
    except Exception as error:
        log_event(
            "ERROR",
            "migration.report.write_failed",
            exception=error,
            invocation_id=report.invocation_id,
            migration_id=report.migration_id,
            error_type=type(error).__name__,
        )


def _print_summary(report_path: Path, report: MigrationReport) -> None:
    print(f"report: {report_path}")
    print(f"diagnostics: {len(report.diagnostics)}, errors: {report.has_errors}")


if __name__ == "__main__":
    sys.exit(main())
