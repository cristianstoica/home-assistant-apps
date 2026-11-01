"""The retired Backfill and Catch up routes (switch-over plan 7.3).

Both answer 410 with the retirement message whatever the daily close's
state: path, method and the mutation guard stay, nothing is read, so an
unknown site also gets 410. No job is queued and the database is not opened.
"""

from __future__ import annotations

from fastapi import APIRouter

from wxverify.api.errors import ApiError
from wxverify.worker.cutover import RETIRED_CONTROL_MESSAGE

router = APIRouter(prefix="/api", tags=["backfill"])


@router.post("/sites/{site_id}/backfill", response_model=None)
async def backfill_site(site_id: int) -> None:
    """Retired: 410 with ``RETIRED_CONTROL_MESSAGE``, for any ``site_id``."""
    raise ApiError(410, RETIRED_CONTROL_MESSAGE)


@router.post("/catchup", response_model=None)
async def catchup() -> None:
    """Retired: 410 with ``RETIRED_CONTROL_MESSAGE``."""
    raise ApiError(410, RETIRED_CONTROL_MESSAGE)
