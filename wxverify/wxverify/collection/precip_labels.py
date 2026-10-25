"""The rain-label boundary: which stored rain samples carry hour-start labels.

A stored rain value at ``valid_at = H`` is the rain in the hour that starts
at H. Open-Meteo and meteoblue label rain by the hour's end, and their
adapters convert it (``precip_hour_start``). Rows those adapters stored
before the conversion keep their end-of-hour labels and are never
relabelled; the boot migration records the newest such ``fetched_at`` under
``PRECIP_HOUR_START_BOUNDARY_KEY`` (``record_precip_hour_start_boundary``),
and every read that pairs, scores or serves their rain keeps only rows
fetched after it.

Standard library only, so any layer may import it.
"""

from __future__ import annotations

from typing import Final

#: ``runtime_state`` key holding the boundary stamp. Its presence is also the
#: migration step's gate; a missing key leaves every affected row out.
PRECIP_HOUR_START_BOUNDARY_KEY: Final = "precip_hour_start_boundary"


def precip_label_eligible_sql(sample: str, feed: str) -> str:
    """SQL predicate: the sample's rain label is safe to pair, score or serve.

    ``sample`` aliases ``forecast_samples`` and ``feed`` aliases ``feeds``.
    Only real Open-Meteo and meteoblue rain is restricted: such a row passes
    only when its ``fetched_at`` is strictly after the boundary stamp, in
    ``julianday`` order. A NULL or unparseable stamp, or a missing key,
    fails closed. The subquery names no outer column, so SQLite runs it once
    per statement.
    """
    return f"""(
        {sample}.variable != 'precip'
        OR {feed}.source NOT IN ('open-meteo', 'meteoblue')
        OR {feed}.is_virtual != 0
        OR COALESCE(
            julianday({sample}.fetched_at) > julianday((
                SELECT value FROM runtime_state
                WHERE key = '{PRECIP_HOUR_START_BOUNDARY_KEY}'
            )),
            0
        )
    )"""
