"""Stream a read-only bancho.py MySQL source database."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Sequence

import pymysql
from pymysql.cursors import DictCursor, SSDictCursor
from sqlalchemy.engine import make_url

from tools.migration.models import SourceRow, SourceSchema
from tools.migration.schema import source_table_names

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class BanchoSource:
    """Own one read-only MySQL connection and expose bounded deterministic reads."""

    def __init__(self, database_url: str) -> None:
        """Parse a MySQL URL without opening the connection."""
        self._url = make_url(database_url)
        self._connection: pymysql.Connection[DictCursor] | None = None

    def __enter__(self) -> BanchoSource:
        """Open a UTF-8 read-only connection."""
        if self._url.database is None:
            raise ValueError("source URL must include a database name")
        self._connection = pymysql.connect(
            host=self._url.host or "127.0.0.1",
            port=self._url.port or 3306,
            user=self._url.username or "",
            password=self._url.password or "",
            database=self._url.database,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=DictCursor,
            read_timeout=120,
            write_timeout=30,
        )
        with self._connection.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Rollback the read-only snapshot and close the source connection."""
        if self._connection is not None:
            self._connection.rollback()
            self._connection.close()
            self._connection = None

    def inspect_schema(self) -> SourceSchema:
        """Read columns, source version, row counts, and a stable source fingerprint."""
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position"
            )
            table_columns: dict[str, set[str]] = {}
            for row in cursor.fetchall():
                table_columns.setdefault(str(row["table_name"]), set()).add(str(row["column_name"]))

        tables = {name: frozenset(columns) for name, columns in table_columns.items()}
        version = self._latest_version(tables)
        counts = {table: self.count(table) for table in sorted(source_table_names()) if table in tables}
        fingerprint_payload = {
            "database": self._url.database,
            "version": version,
            "tables": {name: sorted(columns) for name, columns in sorted(tables.items())},
            "counts": counts,
        }
        encoded = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
        return SourceSchema(tables, version, counts, hashlib.sha256(encoded).hexdigest())

    def count(self, table: str) -> int:
        """Count rows in one validated source table."""
        table_name = _identifier(table)
        with self._require_connection().cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) AS count FROM `{table_name}`")  # noqa: S608 - identifier is validated.
            row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)

    def maximum(self, table: str, column: str) -> int:
        """Return the nonnegative maximum integer value of one source column."""
        table_name = _identifier(table)
        column_name = _identifier(column)
        with self._require_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT COALESCE(MAX(`{column_name}`), 0) AS maximum FROM `{table_name}`"  # noqa: S608
            )
            row = cursor.fetchone()
        value = row["maximum"] if row is not None else 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise RuntimeError(f"source maximum {table}.{column} is not an integer")
        return value

    def fetch_all(
        self,
        table: str,
        *,
        columns: Sequence[str] = ("*",),
        order_by: Sequence[str] = (),
        where: str | None = None,
        parameters: Sequence[object] = (),
    ) -> list[SourceRow]:
        """Fetch a bounded catalog table using validated identifiers."""
        statement = _select_statement(table, columns, order_by, where)
        with self._require_connection().cursor() as cursor:
            cursor.execute(statement, parameters)
            return [dict(row) for row in cursor.fetchall()]

    def iter_batches(
        self,
        table: str,
        *,
        key: str,
        batch_size: int,
        start_after: int = 0,
        columns: Sequence[str] = ("*",),
    ) -> Iterator[list[SourceRow]]:
        """Stream key-ordered rows without OFFSET or unbounded client buffering."""
        table_name = _identifier(table)
        key_name = _identifier(key)
        selected = ", ".join("*" if value == "*" else f"`{_identifier(value)}`" for value in columns)
        cursor_value = start_after
        connection = self._require_connection()
        while True:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT `{key_name}` FROM `{table_name}` WHERE `{key_name}` > %s "  # noqa: S608
                    f"ORDER BY `{key_name}` LIMIT %s",
                    (cursor_value, batch_size),
                )
                boundaries = cursor.fetchall()
            if not boundaries:
                return
            boundary = int(boundaries[-1][key])
            with connection.cursor(SSDictCursor) as cursor:
                cursor.execute(
                    f"SELECT {selected} FROM `{table_name}` WHERE `{key_name}` > %s "  # noqa: S608
                    f"AND `{key_name}` <= %s ORDER BY `{key_name}`",
                    (cursor_value, boundary),
                )
                rows = [dict(row) for row in cursor.fetchall()]
            yield rows
            cursor_value = boundary

    def table_exists(self, table: str) -> bool:
        """Return whether the current source database contains a named table."""
        with self._require_connection().cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            return cursor.fetchone() is not None

    def _latest_version(self, tables: dict[str, frozenset[str]]) -> str | None:
        if "startups" not in tables:
            return None
        rows = self.fetch_all(
            "startups",
            columns=("ver_major", "ver_minor", "ver_micro"),
            order_by=("datetime DESC", "id DESC"),
        )
        if not rows:
            return None
        row = rows[0]
        return f"{int(row['ver_major'])}.{int(row['ver_minor'])}.{int(row['ver_micro'])}"

    def _require_connection(self) -> pymysql.Connection[DictCursor]:
        if self._connection is None:
            raise RuntimeError("bancho source is not connected")
        return self._connection


def _identifier(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


def _select_statement(
    table: str,
    columns: Sequence[str],
    order_by: Sequence[str],
    where: str | None,
) -> str:
    table_name = _identifier(table)
    selected = ", ".join("*" if value == "*" else f"`{_identifier(value)}`" for value in columns)
    statement = f"SELECT {selected} FROM `{table_name}`"  # noqa: S608 - identifiers are validated.
    if where is not None:
        statement += f" WHERE {where}"
    if order_by:
        clauses: list[str] = []
        for raw in order_by:
            name, _, direction = raw.partition(" ")
            normalized_direction = direction.upper() if direction else "ASC"
            if normalized_direction not in {"ASC", "DESC"}:
                raise ValueError("invalid ORDER BY direction")
            clauses.append(f"`{_identifier(name)}` {normalized_direction}")
        statement += f" ORDER BY {', '.join(clauses)}"
    return statement
