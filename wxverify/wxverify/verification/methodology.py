"""Methodology constants for the forecast-product backtest (plan §17).

Every constant is pre-declared and versioned here — one module, one
``METHODOLOGY_VERSION`` — and is never tuned after viewing scores. The
daily record (``record.py``), the daily close's scoring and publication
(``scoring/close_scores.py``, ``scoring/publication.py``), the
truth-coverage rules (``coverage.py``) and the scheduler
import from this module, so the whole methodology is auditable in one
place.

Every public constant here is either consumed by a non-test module or
named in ``DECLARATIVE_ONLY`` — published for the audit trail but wired to
no behavior. A module-scan test pins that disjunction, so a constant can
never again sit here unconsumed and undeclared.

Names and values follow the §17 table, which now holds only the rows
still in use.
"""

from __future__ import annotations

#: Constants published here that are deliberately wired to NO behavior.
#: Empty is the honest state and the desirable one: the sensitivity
#: exhibits are deferred, so their alternate-value tuples were deleted
#: rather than left declared-and-unwired. Anything added here must be
#: justified in the comment beside its definition.
DECLARATIVE_ONLY: frozenset[str] = frozenset()

# Version of this constant set. Bump when ANY constant below changes, and
# equally when what a scored quantity MEANS changes — the date set a
# comparison is computed on, for instance — because a score made under the
# new rule is not comparable with one made under the old, whatever the
# constants say; each publication and forecast record stores the version it
# was made under.
# 3: the served wind became the accuracy-weighted daily high (0.16.6).
# 4: forecast rain is labelled by the start of its hour.
# 5: scores come only from the daily close's scored round and its publications.
METHODOLOGY_VERSION = 5

# Canonical snapshot time (default): the daily decision instant T, expressed
# as site-local wall-clock time.
SNAPSHOT_LOCAL_TIME = "07:00"

# Late-write window for record reconstruction: hours after T during which a
# missing record may still be reconstructed; later => `missed` (gap-scan
# write, §14).
LATE_WRITE_WINDOW_HOURS = 24

# Truth coverage gate (temp high/low): minimum distinct covered hours in the
# local day, plus REQUIRED coverage inside the peak windows (local hours).
TEMP_TRUTH_MIN_HOURS = 18
TEMP_PEAK_WINDOW_HIGH = (12, 18)  # local hours, afternoon maximum
TEMP_PEAK_WINDOW_LOW = (3, 9)  # local hours, overnight minimum

# Near-complete gate (wind_max, precip_total, dry-day): at least
# expected slots minus this allowance.
NEAR_COMPLETE_SLOT_ALLOWANCE = 1
