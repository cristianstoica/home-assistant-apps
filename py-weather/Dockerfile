# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

# Unbuffered stdio so each diagnostic line (stderr) reaches the HA add-on Log tab
# immediately — stdout is block-buffered without a TTY.
# PYTHONPATH=/app lets `python3 -m pyweather` find the package copied below.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy s6-overlay service tree (run/finish for the py-weather service).
COPY rootfs /

# Copy the Python poller package.
COPY pyweather/ /app/pyweather/

LABEL \
    org.opencontainers.image.title="Home Assistant App: Py-Weather" \
    org.opencontainers.image.description="Adaptive poller for Weather.com PWS REST sensors via homeassistant.update_entity (Supervisor Core-API proxy)." \
    org.opencontainers.image.source="https://github.com/cristianstoica/home-assistant-apps" \
    org.opencontainers.image.licenses="MIT"
