# syntax=docker/dockerfile:1.7

# ---------- builder: install deps + bake DuckDB extensions ----------
FROM python:3.12-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /usr/local/bin/

WORKDIR /app

# Project metadata first so Docker can cache the dep layer until pyproject/lock change.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Production install, no dev extras, into /app/.venv.
RUN uv sync --frozen --no-dev --no-editable --extra runner

# Pre-install DuckDB iceberg + httpfs extensions so the runtime never hits
# the network on cold start (CLAUDE.md invariant #6).
RUN /app/.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect(":memory:")
con.install_extension("iceberg")
con.install_extension("httpfs")
con.close()
PY

# ---------- runtime: minimal image with venv + extensions ----------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system --gid 1000 awebo \
    && useradd  --system --uid 1000 --gid awebo --create-home --shell /usr/sbin/nologin awebo

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.duckdb /home/awebo/.duckdb

RUN chown -R awebo:awebo /home/awebo/.duckdb /app

USER awebo

ENTRYPOINT ["python", "-m", "awebo"]
CMD []
