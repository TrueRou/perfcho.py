"""Load and validate migration command configuration."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import orjson


@dataclass(frozen=True, slots=True)
class AccountOverride:
    """Resolve or normalize one source account explicitly."""

    target_account_id: int | None = None
    display_name: str | None = None
    email: str | None = None
    skip: bool = False


@dataclass(frozen=True, slots=True)
class MigrationOverrides:
    """Carry reviewed conflict decisions keyed by bancho.py account ID."""

    accounts: dict[int, AccountOverride] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path | None) -> MigrationOverrides:
        """Load a bounded JSON override document, or return an empty document."""
        if path is None:
            return cls()
        payload = orjson.loads(path.read_bytes())
        if not isinstance(payload, dict) or not isinstance(payload.get("accounts", {}), dict):
            raise ValueError("migration overrides must contain an accounts object")
        accounts: dict[int, AccountOverride] = {}
        for raw_id, raw_override in payload.get("accounts", {}).items():
            if not isinstance(raw_override, dict):
                raise ValueError(f"account override {raw_id!r} must be an object")
            source_id = int(raw_id)
            target_id = raw_override.get("target_account_id")
            accounts[source_id] = AccountOverride(
                target_account_id=int(target_id) if target_id is not None else None,
                display_name=_optional_string(raw_override.get("display_name")),
                email=_optional_string(raw_override.get("email")),
                skip=bool(raw_override.get("skip", False)),
            )
        return cls(accounts)


@dataclass(frozen=True, slots=True)
class MigrationConfig:
    """Describe one reproducible migration run without exposing credentials in output."""

    source_url: str
    target_url: str
    data_directory: Path
    migration_id: str
    source_timezone: ZoneInfo
    batch_size: int = 1000
    report_path: Path = Path("bancho-migration-report.json")
    overrides_path: Path | None = None
    confirm_offline: bool = False

    def __post_init__(self) -> None:
        """Reject unsafe or ambiguous command configuration."""
        if not self.source_url.startswith("mysql"):
            raise ValueError("source URL must use MySQL")
        if not self.target_url.startswith("postgresql+asyncpg"):
            raise ValueError("target URL must use postgresql+asyncpg")
        if not self.migration_id or len(self.migration_id) > 64:
            raise ValueError("migration ID must contain between 1 and 64 characters")
        if any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
            for character in self.migration_id
        ):
            raise ValueError("migration ID may only contain letters, numbers, dots, dashes, and underscores")
        if not 1 <= self.batch_size <= 10_000:
            raise ValueError("batch size must be between 1 and 10000")

    @property
    def overrides(self) -> MigrationOverrides:
        """Load reviewed overrides only when they are needed."""
        return MigrationOverrides.load(self.overrides_path)

    @property
    def digest(self) -> str:
        """Return a non-secret digest used to reject incompatible resumes."""
        value = {
            "migration_id": self.migration_id,
            "source_timezone": self.source_timezone.key,
            "batch_size": self.batch_size,
            "data_directory": str(self.data_directory.resolve()),
            "overrides_sha256": _file_digest(self.overrides_path),
        }
        encoded = orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def from_values(
        cls,
        *,
        source_url: str | None,
        target_url: str | None,
        data_directory: Path,
        migration_id: str,
        source_timezone: str,
        batch_size: int,
        report_path: Path,
        overrides_path: Path | None,
        confirm_offline: bool,
    ) -> MigrationConfig:
        """Resolve URL environment fallbacks and an IANA source timezone."""
        resolved_source = source_url or os.getenv("BANCHO_DATABASE_URL")
        resolved_target = target_url or os.getenv("DATABASE_URL")
        if not resolved_source:
            raise ValueError("BANCHO_DATABASE_URL or --source-url is required")
        if not resolved_target:
            raise ValueError("DATABASE_URL or --target-url is required")
        try:
            timezone = ZoneInfo(source_timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown source timezone: {source_timezone}") from error
        return cls(
            source_url=resolved_source,
            target_url=resolved_target,
            data_directory=data_directory,
            migration_id=migration_id,
            source_timezone=timezone,
            batch_size=batch_size,
            report_path=report_path,
            overrides_path=overrides_path,
            confirm_offline=confirm_offline,
        )


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("override string values must not be empty")
    return value.strip()


def _file_digest(path: Path | None) -> str | None:
    if path is None:
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()
