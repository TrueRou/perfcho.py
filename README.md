# perfcho.py

Central runtime foundation for a unified osu! Stable and Lazer server.

The current delivery defines the canonical PostgreSQL domain model, Redis online-state boundary, and Taskiq transactional
outbox runtime. Stable/Lazer protocol services will be implemented against this contract in later deliveries.

## Requirements

- Python 3.14t
- uv
- PostgreSQL 17 and Redis 8, normally started through Docker Compose

## Development dependencies

```bash
docker compose up -d postgres redis
uv run alembic upgrade head
```

PostgreSQL listens on `127.0.0.1:55432`; Redis listens on `127.0.0.1:56379`. Copy `.env.example` to `.env` only when local
values need to be overridden.

Run the process roles separately:

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run taskiq worker perfcho.infra.taskiq:broker perfcho.tasks.outbox --ack-type when_executed
uv run python -m perfcho.infra.outbox
```

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
src/perfcho/infra/outbox.py  PostgreSQL delivery ledger and Taskiq relay
src/perfcho/infra/taskiq.py  Redis Stream broker and worker lifecycle
alembic/versions/         Immutable schema migrations
.agent-space/docs/        Chinese architecture, operations, and business contracts
tests/                    Metadata and PostgreSQL migration tests
```

See [the database architecture](.agent-space/docs/database/architecture.md) before changing a model or adding a
business workflow.
