# perfcho.py

Central runtime foundation for a unified osu! Stable and Lazer server.

The current delivery defines the canonical PostgreSQL domain model, Redis online-state boundary, and Taskiq transactional
outbox runtime. Stable/Lazer protocol services will be implemented against this contract in later deliveries.

## Requirements

- Python 3.14t
- uv
- Docker Compose V2
- PostgreSQL 17, Redis 8, and S3-compatible object storage, normally started through Docker Compose

## Development dependencies

```bash
docker compose up -d --wait postgres redis minio
docker compose run --rm --no-deps minio-init
```

PostgreSQL listens on `127.0.0.1:55432`; Redis listens on `127.0.0.1:56379`; MinIO listens on `127.0.0.1:59000`, with its
console on `127.0.0.1:59001`. Copy `.env.example` to `.env` only when local values need to be overridden.
The first application role to connect creates missing PostgreSQL schemas and mapped tables through SQLAlchemy
`MetaData.create_all()`.

### VS Code

Select `perfcho: all processes` in Run and Debug and press F5. The compound launch configuration synchronizes locked
dependencies, starts and waits for PostgreSQL, Redis, and MinIO, initializes the object-storage bucket, and then debugs
these roles in parallel:

- API
- Outbox Relay
- Taskiq Worker

Stopping one debug session stops all three application roles. PostgreSQL, Redis, and MinIO remain running so that
subsequent debug sessions start quickly; stop them explicitly with `docker compose down` when they are no longer needed.

Run the process roles separately:

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run taskiq worker perfcho.infra.taskiq:broker perfcho.tasks.outbox --ack-type when_executed
uv run python -m perfcho.infra.outbox
```

## Production Compose

`compose.prod.yaml` is a standalone production topology; do not merge it with the development `compose.yaml`. Create a
production environment file from `.env.production.example`, replace every credential and signing key, configure the
external S3-compatible object store, and start the deployment:

```bash
openssl rand -hex 32
docker compose --env-file .env.production -f compose.prod.yaml up -d --build
docker compose --env-file .env.production -f compose.prod.yaml ps
```

The production topology runs PostgreSQL, authenticated Redis, API, Outbox Relay, and Taskiq Worker. Each application role
ensures missing schemas and tables exist at startup; a PostgreSQL advisory lock serializes concurrent initialization.
S3-compatible object storage remains an external production dependency. Only the API is published, on `127.0.0.1:8000`
by default, for a same-host reverse proxy to terminate TLS. PostgreSQL and Redis use named volumes and are not published
to the host. Back up PostgreSQL and object storage consistently; Redis AOF is only an availability aid and is not the
source of durable task truth.

`create_all()` does not alter existing columns, constraints, or indexes. Model changes that affect existing databases
still require an explicit operational rollout.

## Verification

```bash
uv run ruff format .
uv run ruff check .
uv run pytest
TEST_DATABASE_URL=postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho_test uv run pytest -m postgres
```

## Structure

```text
src/perfcho/infra/db/
  base.py                 SQLAlchemy metadata and schema registry
  enums.py                Stable cross-domain values
  mixins.py               ID and timestamp policies
  models/                 Domain-separated models with English purpose docstrings
src/perfcho/infra/outbox.py  PostgreSQL delivery ledger and Taskiq relay
src/perfcho/infra/taskiq.py  Redis Stream broker and worker lifecycle
.agent-space/docs/        Chinese architecture, operations, and business contracts
tests/                    Infrastructure and domain contract tests
```

See [the database architecture](.agent-space/docs/database/architecture.md) before changing a model or adding a
business workflow.
