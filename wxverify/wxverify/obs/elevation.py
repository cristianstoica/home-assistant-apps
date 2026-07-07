"""Open-Meteo Elevation DEM lookup."""

from __future__ import annotations

from typing import Any, cast

import httpx


async def lookup_elevation_m(lat: float, lon: float) -> float:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://api.open-meteo.com/v1/elevation",
            params={"latitude": lat, "longitude": lon},
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
        response.raise_for_status()
        data = cast(dict[str, Any], response.json())
    elevation = data.get("elevation")
    if isinstance(elevation, list) and elevation:
        first = cast(list[object], elevation)[0]
        if isinstance(first, str | int | float):
            return float(first)
    if isinstance(elevation, int | float):
        return float(elevation)
    raise RuntimeError("Open-Meteo Elevation response missing elevation")
