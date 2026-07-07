"""Deterministic hashing helpers used by the scheduler."""

from __future__ import annotations

import hashlib


def obs_jitter_minutes(
    site_id: int, cycle_bucket: int, jitter_cap: int, *, seed: int = 1729
) -> int:
    if jitter_cap <= 0:
        return 0
    digest = hashlib.blake2b(
        f"{seed}:{site_id}:{cycle_bucket}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big") % (jitter_cap + 1)
