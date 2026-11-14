# Dashboard service image: `make dashboard-docker` or `docker compose --profile dashboard up`.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app

# Dependencies first so code changes don't reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY sql ./sql
COPY migrations ./migrations
RUN uv sync --frozen --no-dev

RUN useradd --system --no-create-home app
USER app

EXPOSE 8050
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8050/healthz', timeout=3)"
# Inside the container 0.0.0.0 is required; compose publishes the port on localhost only.
CMD ["/app/.venv/bin/ch-etl", "dashboard", "--host", "0.0.0.0", "--port", "8050"]
