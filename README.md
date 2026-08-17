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

The repository has separate development and production Compose topologies. `compose.yaml` starts local PostgreSQL,
Redis, MinIO, Loki, and Grafana infrastructure; the API, Outbox Relay, and Taskiq Worker run as host processes and read
`.env`. `compose.prod.yaml` runs the same infrastructure plus the perfcho-pp Calculator, API, Outbox Relay, and Taskiq
Worker in containers. The application roles share one Python image in production.

### Environment files

- `.env.example` — complete local contract with working development values. Copy it to `.env` for local development.
- `.env.production.example` — production contract for `compose.prod.yaml`; copy it to `.env.production` and replace every
  placeholder before deployment.

```bash
cp .env.example .env
docker compose --env-file .env up -d --wait postgres redis minio loki grafana
docker compose --env-file .env run --rm --no-deps minio-init
```

The development infrastructure exposes PostgreSQL on `127.0.0.1:55432`, Redis on `127.0.0.1:56379`, MinIO on
`127.0.0.1:59000`, Loki on `127.0.0.1:53100`, and Grafana on `127.0.0.1:53000`. Grafana uses `perfcho` /
`perfcho-development` locally and provisions Loki as its default data source. The host-run application roles send
structured logs directly to Loki while their consoles always use human-readable output.
Grafana opens the provisioned `Perfcho Logs` dashboard by default; it is also available directly at
`http://127.0.0.1:53000/d/perfcho-logs/perfcho-logs`.
The dashboard provides a level histogram, text search, environment/process/level filters, and concise log rows. Grafana
renders time and stream labels in the UI while expandable row details retain business context from the JSON payload.
The first application role to connect creates missing PostgreSQL schemas and mapped tables through SQLAlchemy
`MetaData.create_all()`; a PostgreSQL advisory lock serializes concurrent initialization. `create_all()` does not alter
existing columns, constraints, or indexes, so model changes that affect an existing database still require an explicit
operational rollout.

## Local development

Start the infrastructure and run the application roles as host processes:

```bash
cp .env.example .env
docker compose up -d --wait postgres redis minio loki grafana
docker compose run --rm --no-deps minio-init
```

PostgreSQL listens on `127.0.0.1:55432`; Redis on `127.0.0.1:56379`; MinIO on `127.0.0.1:59000`, with its console on
`127.0.0.1:59001`. Loki listens on `127.0.0.1:53100` and Grafana on `127.0.0.1:53000`. The host processes read the
same configuration from `.env`.

### VS Code

Select `perfcho: all processes` in Run and Debug and press F5. The compound launch configuration synchronizes locked
dependencies, creates `.env` from `.env.example` when it is missing, starts and waits for PostgreSQL, Redis, MinIO,
Loki, and Grafana,
initializes the object-storage bucket, and then debugs these roles in parallel:

- API
- Outbox Relay
- Taskiq Worker

Stopping one debug session stops both application roles. PostgreSQL, Redis, MinIO, Loki, and Grafana remain running so that
subsequent debug sessions start quickly; stop them explicitly with `docker compose down` when they are no longer needed.

Run the process roles separately:

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run python -m perfcho.relay
uv run taskiq worker perfcho.worker:broker --ack-type when_executed
```

### Trace correlation

Each HTTP request receives a 128-bit trace ID. A valid W3C `traceparent` is inherited; otherwise the API generates a new
trace and returns it in `X-Trace-ID`. The console prints only time, level, process role, trace ID, and event/message. All
business context remains in Loki.

Events with `duration_ms` append their execution time. Worker Outbox logs also append the durable `event_type` and
`delay_ms`, measured from Outbox event creation until the Taskiq consumer starts.

The trace is persisted on the Outbox event and propagated through relay, Taskiq, consumer retries, and downstream Outbox
events. Paste the CLI or response trace into the Grafana dashboard's `Trace ID` field to isolate the complete chain.

## Production Compose

`compose.prod.yaml` is a standalone production topology and includes internal MinIO:

```bash
cp .env.production.example .env.production
# Replace all replace-with-* values, then:
docker compose --env-file .env.production -f compose.prod.yaml up -d --build
docker compose --env-file .env.production -f compose.prod.yaml ps
```

The API is published on `127.0.0.1:10727` and Grafana on `127.0.0.1:53000` by default for same-host access. PostgreSQL
and Redis retain their existing published ports; MinIO and Loki stay internal. Loki keeps logs for 30 days; Grafana's
Loki data source is provisioned automatically. Set a strong `GRAFANA_ADMIN_PASSWORD` before deployment.
The same provisioned `Perfcho Logs` dashboard is configured as the Grafana home dashboard.

In Grafana Explore, start with `{application="perfcho"}` and refine by labels such as `environment`, `process_role`,
and `level`. Trace and fixed source fields use Loki structured metadata; caller-provided business fields remain in the
JSON log line without filtering or redaction.

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
