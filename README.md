# perfcho.py

Central runtime foundation for a unified osu! Stable and Lazer server.

The current delivery defines the canonical PostgreSQL domain model, Redis online-state boundary, and Taskiq transactional
outbox runtime. Stable/Lazer protocol services will be implemented against this contract in later deliveries.

## Requirements

- Python 3.14
- uv
- Docker Compose V2
- PostgreSQL 17, Redis 8, and S3-compatible object storage, normally started through Docker Compose

## Runtime Environment

The repository has separate development and production Compose topologies. `compose.yaml` starts only local
PostgreSQL, Redis, and MinIO infrastructure; the API and Taskiq Worker run as host processes and read `.env`.
`compose.prod.yaml` runs PostgreSQL, authenticated Redis, internal MinIO, the perfcho-pp Calculator, the API, and the
Taskiq Worker in containers. The two application roles share one Python image in production.

### Environment files

- `.env.example` — complete local contract with working development values. Copy it to `.env` for local development.
- `.env.production.example` — production contract for `compose.prod.yaml`; copy it to `.env.production` and replace every
  placeholder before deployment.

```bash
cp .env.example .env
docker compose --env-file .env up -d --wait postgres redis minio
docker compose --env-file .env run --rm --no-deps minio-init
```

The development infrastructure exposes PostgreSQL on `127.0.0.1:55432`, Redis on `127.0.0.1:56379`, and MinIO on
`127.0.0.1:59000`; the host-run application roles read the same local endpoints and credentials from `.env`.
The first application role to connect creates missing PostgreSQL schemas and mapped tables through SQLAlchemy
`MetaData.create_all()`; a PostgreSQL advisory lock serializes concurrent initialization. `create_all()` does not alter
existing columns, constraints, or indexes, so model changes that affect an existing database still require an explicit
operational rollout.

## Local development

Start only the infrastructure (PostgreSQL, Redis, MinIO) and run the application roles as host processes:

```bash
cp .env.example .env
docker compose up -d --wait postgres redis minio
docker compose run --rm --no-deps minio-init
```

PostgreSQL listens on `127.0.0.1:55432`; Redis on `127.0.0.1:56379`; MinIO on `127.0.0.1:59000`, with its console on
`127.0.0.1:59001`. The host processes read the same secrets from `.env`.

### VS Code

Select `perfcho: all processes` in Run and Debug and press F5. The compound launch configuration synchronizes locked
dependencies, creates `.env` from `.env.example` when it is missing, starts and waits for PostgreSQL, Redis, and MinIO,
initializes the object-storage bucket, and then debugs these roles in parallel:

- API
- Taskiq Worker, including the durable Outbox and Performance relay loop

Stopping one debug session stops both application roles. PostgreSQL, Redis, and MinIO remain running so that
subsequent debug sessions start quickly; stop them explicitly with `docker compose down` when they are no longer needed.

Run the process roles separately:

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run taskiq worker perfcho.worker:broker --ack-type when_executed
```

## Production Compose

`compose.prod.yaml` is a standalone production topology and includes internal MinIO:

```bash
cp .env.production.example .env.production
# Replace all replace-with-* values, then:
docker compose --env-file .env.production -f compose.prod.yaml up -d --build
docker compose --env-file .env.production -f compose.prod.yaml ps
```

Only the API is published, on `127.0.0.1:10727` by default for a same-host reverse proxy. PostgreSQL, Redis, and MinIO
use named volumes and are not published to the host.

## Verification

### osu.py end-to-end client

The fake client runs the pinned `osu.py` 1.5.4 implementation against real Uvicorn and Taskiq processes. It creates an
isolated Compose project for PostgreSQL, Redis, and MinIO, synchronizes a deterministic beatmap through the production
content service, and tears down only resources owned by that run:

```bash
uv run python -m tools.fakeclient run --artifacts .fakeclient/latest --timeout 15
```

The suite covers ordinary Stable login and Poll, presence, friends, chat, spectator frames, the non-Tourney multiplayer
flow, Direct and Web APIs, comments, score submission, ranking projection, and replay download. Public avatars, covers,
previews, seasonal backgrounds, menu metadata, and `.osz` files are forwarded to a local upstream fixture during E2E;
perfcho does not store those public resources in MinIO. Tourney clients are intentionally outside this suite and server
support boundary.

Connect one existing account to an already running API with:

```bash
uv run python -m tools.fakeclient smoke --base-url http://127.0.0.1:8000 \
  --username player --password 'plain-text-password'
```

```bash
uv run ruff format .
uv run ruff check .
uv run pytest
TEST_DATABASE_URL=postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho_test uv run pytest -m postgres
```

When `TEST_DATABASE_URL` is configured, the PostgreSQL test fixture creates the target database if it does not exist
and resets its application schemas around each test. The configured PostgreSQL role therefore needs `CREATEDB`
permission.

## Bancho Migration

The offline bancho.py v5.2.2 MySQL and asset migration is available through
`python -m tools.migration`. Run `preflight` before `apply`; the operational prerequisites, exclusions, override
format, recovery rules, and verification procedure are documented in
[the migration runbook](.agent-space/docs/bancho-migration.md).

## Structure

See [the database architecture](.agent-space/docs/database/architecture.md) before changing a model or adding a
business workflow.
