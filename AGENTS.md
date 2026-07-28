# Agent Guidelines

## DO THIS BEFORE YOU START

go to check .agent-space's background.md to gain all the information you need to understand the requirements and context. Then, implement the requested feature or functionality according to the overall requirements, background, and goals.

the .agent-space/docs kept tracking on the whole progress of the project, check it out and update it if required.

Notice: we hope to consider lazer(laser) as baseline of the implementation, and stable is like a adapter layer to the lazer structure, this may give you a reference to implement the features, like design lazer ahead of stable, if user let you to build stable first, you can build stable first, but you should consider the design of lazer in your implementation.

## FastAPI Module and API Style

Use [TrueRou/fastapi-perfectionist-starter](https://github.com/TrueRou/fastapi-perfectionist-starter) as the structural and naming reference for new FastAPI code. The conventions below were verified against commit `2898eb17b31c5a1308059cbe71a6c66bf61c412d` (2026-07-27). Treat it as a code-style reference; this repository's `.agent-space/background.md` and `.agent-space/docs/` remain the architecture authority.

### Reference Layout

```text
src/<package>/
├── modules/
│   └── <domain>/
│       ├── __init__.py
│       ├── services.py
│       └── dependencies.py       # optional; only when reusable FastAPI dependencies exist
└── api/
    ├── __init__.py               # includes each API version router
    └── v1/
        ├── router/
        │   ├── __init__.py       # owns the /v1 prefix and includes domain routers
        │   └── <domain>.py
        └── schema/
            ├── __init__.py
            └── <domain>.py
```

- Name domain packages and files with singular `snake_case`, for example `modules/auth`, `modules/user`, and `router/note.py`.
- Keep `__init__.py` empty unless it composes routers. Do not use package initializers as miscellaneous export surfaces.
- Start a small domain with `services.py` and add `dependencies.py` only when the dependency is reused or performs meaningful authentication, resource loading, or authorization. Split further only after the domain outgrows these files.
- Keep API versions structurally independent. A versioned schema is a transport contract, not a business model.

### File Responsibilities

- `modules/<domain>/services.py`: application operations and persistence coordination. Use a `<Domain>Service` class, async methods for I/O, constructor injection, and explicit return annotations.
- `modules/<domain>/dependencies.py`: callable dependency classes such as `RequireAuthUser` or `RequireNote`. They resolve authenticated actors or resources and enforce endpoint-level access preconditions shared by multiple routes.
- `api/v1/schema/<domain>.py`: Pydantic request and response types. Use role suffixes such as `CreateRequest`, `UpdateRequest`, and `Response`; put transport validation in `Field` declarations and never expose secret persistence fields.
- `api/v1/router/<domain>.py`: one `router = APIRouter(...)` per HTTP domain. Routers parse transport input, inject dependencies, invoke one application operation, and serialize the result. They do not contain SQL or business workflows.
- `api/v1/router/__init__.py`: import each domain router as `<domain>_router`, create `APIRouter(prefix="/v1")`, and include domain routers explicitly.
- `api/__init__.py`: compose version routers only. The FastAPI application includes this top-level router once.

### Dependency and Endpoint Conventions

- Preserve the dependency direction `api router -> api schema + module service/dependency -> infrastructure`; modules must never import from `api`.
- Use `Annotated[T, Depends(...)]`, `Annotated[T, Body()]`, and `Annotated[T, Query()]` instead of untyped or hidden parameter wiring.
- Use `srv_<domain>` for injected services, `dep_<resource>` for resolved dependencies, `body` for request bodies, and `params` for query parameter objects.
- Give domain routers a resource prefix and tag, normally plural nouns such as `APIRouter(prefix="/notes", tags=["notes"])`.
- Declare `response_model` explicitly. Managed JSON APIs should use the shared typed response envelope and pagination contracts in `api/v1/response.py` and `api/v1/pagination.py`.
- Keep endpoint bodies short: read validated inputs, call a service, and return the typed response. Reusable current-user and resource ownership checks belong in dependencies rather than being duplicated in endpoints.
- Services may depend on other services through constructor injection. Avoid constructing sessions, repositories, or services inside endpoint and service methods.

### Perfcho Architecture Overrides

The reference starter is intentionally small and couples parts of the business layer to FastAPI and SQLAlchemy. Do not copy those couplings into perfcho:

- Design canonical Lazer-facing commands, queries, and business models first. Stable HTTP/Bancho handlers are protocol adapters over the same module interfaces, not separate business implementations.
- API and realtime adapters may not import SQLAlchemy models, issue queries, call `commit()`, or own transactions. They depend on typed application commands and query services as required by `.agent-space/docs/business-layer.md`.
- Module services must raise application/domain errors rather than `HTTPException`. The API middleware or adapter maps those errors to protocol-specific responses.
- Do not return ORM entities as API contracts. Map application results to versioned response schemas at the adapter boundary.
- Use an application-owned `AsyncSession` transaction per command and write required business facts plus outbox records atomically. Do not reproduce the starter's request dependency that commits automatically after `yield`.
- Keep database models in perfcho's existing `infra/db/models/<schema>.py` layout; do not recreate the starter's single `infra/models.py` file.
- Dependencies may authenticate and normalize protocol input, but authorization decisions based on authoritative state belong in the application service so workers and non-HTTP adapters enforce the same policy.

### Adding a Domain

1. Add `modules/<domain>/services.py` and, when needed, `dependencies.py`.
2. Add the protocol-specific request/response models under `api/<version>/schema/<domain>.py`.
3. Add a thin domain router under `api/<version>/router/<domain>.py`.
4. Register the router explicitly in that version's `router/__init__.py`.
5. Add service tests for business behavior and API tests for validation, dependency wiring, error translation, and response contracts.

## After Every Code Change

Run ruff formatting and auto-fix after every file modification:

```bash
uv run ruff format .
uv run ruff check --fix .
```

## Refactoring

- When refactoring, always update the corresponding tests in `tests/` to match the new structure/signatures.
- Do not leave tests that reference old APIs or removed code.
- Backward compatibility is generally not required — rename, restructure, and break interfaces freely when it improves the codebase.
