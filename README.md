# perfcho.py

PostgreSQL persistence foundation for a unified osu! Stable and Lazer server.

The current delivery is intentionally database-only. It defines the canonical domain model, migrations, constraints,
indexes, projections, and trust boundaries. Stable/Lazer protocol services will be implemented against this contract in
a later delivery.

## Requirements

- Python 3.14t
- uv
- PostgreSQL 17, normally started through Docker Compose

## Development database

```bash
docker compose up -d postgres
uv run alembic upgrade head
```

The Compose service listens on `127.0.0.1:55432`. Copy `.env.example` to `.env` only when local values need to be
overridden.

## Verification

```bash
uv run ruff format .
uv run ruff check .
uv run pytest
TEST_DATABASE_URL=postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho_test uv run pytest -m postgres
uv run alembic check
```

## Structure

```text
src/perfcho/infra/database/
  base.py                 SQLAlchemy metadata and schema registry
  enums.py                Stable cross-domain values
  mixins.py               ID and timestamp policies
  models/                 Domain-separated models with English purpose docstrings
alembic/versions/         Immutable schema migrations
.agent-space/docs/        Chinese architecture, operations, and business contracts
tests/                    Metadata and PostgreSQL migration tests
```

See [the database architecture](.agent-space/docs/database/architecture.md) before changing a model or adding a
business workflow.
