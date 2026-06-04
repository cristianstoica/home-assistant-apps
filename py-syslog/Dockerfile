FROM python:3.13-alpine

# HA add-on labels are required when not building via the official GitHub builder
# action. Supervisor passes BUILD_VERSION (from config.yaml) and BUILD_ARCH; the
# defaults keep a standalone `docker build` working off-HAOS.
ARG BUILD_VERSION=dev
ARG BUILD_ARCH=amd64
LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="${BUILD_ARCH}"

# Unbuffered stdio so the stdout echo (stored lines) and stderr diagnostics reach the HA
# Log tab immediately — container stdout is block-buffered when stdout is not a TTY.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY pysyslog/ ./pysyslog/
CMD ["python3", "-m", "pysyslog"]
