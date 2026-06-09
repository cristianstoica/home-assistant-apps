# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

# Unbuffered stdio so each stored line (stdout) and diagnostic (stderr) reaches
# the HA add-on Log tab immediately — stdout is block-buffered without a TTY.
# PYTHONPATH=/app lets `python3 -m pysyslog` find the package copied below.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy s6-overlay service tree (run/finish for the py-syslog service).
COPY rootfs /

# Copy the Python collector package.
COPY pysyslog/ /app/pysyslog/

LABEL \
    org.opencontainers.image.title="Home Assistant App: Py-Syslog" \
    org.opencontainers.image.description="Durable UDP syslog collector for Home Assistant (RFC 3164/5424)." \
    org.opencontainers.image.source="https://github.com/cristianstoica/home-assistant-apps" \
    org.opencontainers.image.licenses="MIT"
