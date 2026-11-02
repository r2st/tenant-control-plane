FROM python:3.13-slim AS base
RUN pip install --no-cache-dir uv==0.11.15
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./

FROM base AS runtime
RUN uv sync --frozen --no-dev --no-install-project && \
    groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app && \
    mkdir /data && chown app:app /data
COPY control_plane ./control_plane
ENV PATH="/app/.venv/bin:$PATH" DATABASE_PATH=/data/control.sqlite3
USER app
CMD ["python", "-m", "control_plane"]

FROM base AS checks
RUN uv sync --frozen --no-install-project
COPY control_plane ./control_plane
COPY tests ./tests
COPY scripts ./scripts
ENV PATH="/app/.venv/bin:$PATH"
CMD ["sh", "scripts/check.sh"]
