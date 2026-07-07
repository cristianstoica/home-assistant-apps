# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY pyproject.toml uv.lock /tmp/

RUN pip install --no-cache-dir uv==0.9.17 && \
    uv export --frozen --no-dev --no-editable -o /tmp/requirements.txt \
        --project /tmp && \
    uv pip install --system --no-cache --only-binary=:all: \
        --index-strategy unsafe-best-match \
        -r /tmp/requirements.txt && \
    uv --version | grep -F "uv 0.9.17" && \
    python3 -m pip freeze | grep -F "fastapi==0.115.14" && \
    python3 -m pip freeze | grep -F "httpx==0.28.1" && \
    python3 -m pip freeze | grep -F "Jinja2==3.1.6" && \
    python3 -m pip freeze | grep -F "pydantic==2.11.7" && \
    python3 -m pip freeze | grep -F "uvicorn==0.34.3" && \
    apk add --no-cache tzdata

COPY rootfs /
COPY wxverify/ /app/wxverify/

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f -s -o /dev/null http://127.0.0.1:8099/api/sites || exit 1

LABEL \
    org.opencontainers.image.title="Home Assistant App: Weather Verify" \
    org.opencontainers.image.description="Forecast verification for Home Assistant using PWS cluster consensus." \
    org.opencontainers.image.source="https://github.com/cristianstoica/home-assistant-apps" \
    org.opencontainers.image.licenses="MIT"
