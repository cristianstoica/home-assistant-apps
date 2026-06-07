# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

# Unbuffered stdio so each status line and diagnostic (stderr) reaches the HA
# add-on Log tab immediately — stdout is block-buffered without a TTY.
# PYTHONPATH=/app lets `python3 -m pyddns` find the package copied below.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy s6-overlay service tree (run/finish for the py-ddns service).
COPY rootfs /

# Copy the Python updater package.
COPY pyddns/ /app/pyddns/

LABEL \
    org.opencontainers.image.title="Home Assistant App: Py-DDNS" \
    org.opencontainers.image.description="Generic multi-provider dynamic-DNS updater for Home Assistant (Azure DNS / callback URL)." \
    org.opencontainers.image.source="https://github.com/cristianstoica/home-assistant-apps" \
    org.opencontainers.image.licenses="MIT"
