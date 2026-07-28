# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.11.29-trixie-slim

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_NO_DEV=1 \
    UV_PYTHON=3.14t \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python

WORKDIR /app

COPY .python-version pyproject.toml uv.lock README.md ./
RUN uv python install 3.14t && \
    uv sync --locked --no-install-project

COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic
RUN uv sync --locked --no-editable && \
    chmod -R a+rX /opt/uv/python /app

USER 10001:10001

EXPOSE 8000

CMD ["uvicorn", "perfcho.main:asgi_app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header"]
