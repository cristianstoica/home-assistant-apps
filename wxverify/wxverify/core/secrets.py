"""Provider secret resolution boundary."""

from __future__ import annotations

from wxverify.core.options import load_runtime_config


def resolve_secret(provider: str) -> str | None:
    return load_runtime_config().secrets.get(provider)


def key_status() -> dict[str, bool]:
    cfg = load_runtime_config()
    return {provider: bool(value) for provider, value in cfg.secrets.items()}
