"""Methodology constants for the forecast-product backtest (plan §17).

Every constant is pre-declared and versioned here — one module, one
``METHODOLOGY_VERSION`` — and is never tuned after viewing scores. Every
constant here is wired into behavior; the record snapshots, backtest
engine, verdicts, and the published contract import from this module so
the whole methodology is auditable in one place.

Naming follows the §17 table row-for-row; values are the table's, verbatim.
"""

from __future__ import annotations

# Version of this constant set. Bump when ANY constant below changes; runs
# persist the version they were scored under.
METHODOLOGY_VERSION = 1

# Canonical snapshot time (default): the daily decision instant T, expressed
# as site-local wall-clock time.
SNAPSHOT_LOCAL_TIME = "07:00"

# Late-write window for record reconstruction: hours after T during which a
# missing record may still be reconstructed; later => `missed` (gap-scan
# write, §14).
LATE_WRITE_WINDOW_HOURS = 24

# Delta_consensus (outcome knowability lag): hours after `valid_at` before an
# outcome counts as knowable. Conservative direction is safe.
CONSENSUS_LAG_HOURS = 3

# Truth coverage gate (temp high/low): minimum distinct covered hours in the
# local day, plus REQUIRED coverage inside the peak windows (local hours).
TEMP_TRUTH_MIN_HOURS = 18
TEMP_PEAK_WINDOW_HIGH = (12, 18)  # local hours, afternoon maximum
TEMP_PEAK_WINDOW_LOW = (3, 9)  # local hours, overnight minimum

# Near-complete gate (wind_max, precip_total, dry-day): at least
# expected slots minus this allowance.
NEAR_COMPLETE_SLOT_ALLOWANCE = 1

# Roster availability floor: share of truth-eligible days on which every
# active real feed must be available. Sensitivity exhibits use the alternates.
ROSTER_AVAILABILITY_FLOOR = 0.70
ROSTER_AVAILABILITY_FLOOR_SENSITIVITY = (0.60, 0.80)

# Adequate lead: minimum strict-common days for a lead to count. Minimum
# number of adequate leads (of D1-D7) per variable.
ADEQUATE_LEAD_MIN_DAYS = 20
MIN_ADEQUATE_LEADS_PER_VARIABLE = 4

# Bootstrap: moving-block over target dates, fixed seed per run.
BOOTSTRAP_BLOCK_LENGTH_DAYS = 3
BOOTSTRAP_BLOCK_LENGTH_SENSITIVITY = (2, 5)
BOOTSTRAP_RESAMPLES = 10_000

# Confidence-interval levels. Candidate CIs are simultaneous at
# alpha = 0.05/3; precip improvement CIs (occurrence/total disjunction) at
# alpha = 0.05/6; baseline-gate CIs are plain 95%.
CANDIDATE_CI_LEVEL = 1 - 0.05 / 3
PRECIP_IMPROVEMENT_CI_LEVEL = 1 - 0.05 / 6
BASELINE_GATE_CI_LEVEL = 0.95

# Lead-stability proportion: at least ceil(2/3 of adequate leads) must agree.
LEAD_STABILITY_NUMERATOR = 2
LEAD_STABILITY_DENOMINATOR = 3

# Practical floor: minimum improvement worth acting on.
PRACTICAL_FLOOR_RELATIVE_MAE = 0.05
PRACTICAL_FLOOR_ETS = 0.05

# Non-inferiority margins (conjunctive screen, §12).
NON_INFERIORITY_MAE_MARGIN = 0.02
NON_INFERIORITY_ETS_MARGIN = 0.02

# Occurrence minimum events: common days required on each side.
OCCURRENCE_MIN_WET_DAYS = 8
OCCURRENCE_MIN_DRY_DAYS = 8

# Daily-rank diagnostic: minimum knowable scorable days per (feed, quantity)
# before ranking; ties break by (source, model) as production does.
DAILY_RANK_MIN_HISTORY_DAYS = 10
