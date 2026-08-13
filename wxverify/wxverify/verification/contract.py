"""Shared §16 response/render contract for the API and the page.

One definition of the wire-contract version, the measurement contract, and
the methodology constant set, imported by BOTH ``/api/verification/*`` and
the ``/verification`` page so the two surfaces cannot drift (the §18.11
API/UI consistency obligation).
"""

from __future__ import annotations

from wxverify.verification import methodology
from wxverify.verification.simulate import SIM_DEPTHS, SIM_VARIABLES

#: Response-contract version for every /api/verification/* payload (§16).
VERIFICATION_SCHEMA = 1

#: Machine-readable measurement contract (§16): units, metric direction,
#: confidence levels, sample definition, and null semantics.
CONTRACT: dict[str, object] = {
    "units": {
        "temperature_high": "degC",
        "temperature_low": "degC",
        "wind_max": "m/s",
        "precip_total": "mm",
        "precip_occurrence": "wet_day_class",
    },
    "metric_direction": {
        "mae": "lower_is_better",
        "rmse": "lower_is_better",
        "bias": "zero_is_best",
        "ets": "higher_is_better",
        "delta_vs_incumbent": "higher_is_better",
    },
    "confidence_levels": {
        "candidate_ci": methodology.CANDIDATE_CI_LEVEL,
        "precip_improvement_ci": methodology.PRECIP_IMPROVEMENT_CI_LEVEL,
        "baseline_gate_ci": methodology.BASELINE_GATE_CI_LEVEL,
    },
    "sample_definition": (
        "one local target day on the cell's strict common core; pairwise "
        "cores for non-headline entities"
    ),
    "null_semantics": (
        "insufficient, not-applicable and failed values are null, never 0"
    ),
    "null_reasons": [
        "no_samples",
        "no_prior_truth",
        "insufficient_rank_history",
        "truth_missing",
    ],
}


def methodology_constants() -> dict[str, object]:
    """The pre-declared §17 constant set, as one JSON-able dict."""
    return {
        "methodology_version": methodology.METHODOLOGY_VERSION,
        "snapshot_local_time_default": methodology.SNAPSHOT_LOCAL_TIME,
        "late_write_window_hours": methodology.LATE_WRITE_WINDOW_HOURS,
        "consensus_lag_hours": methodology.CONSENSUS_LAG_HOURS,
        "temp_truth_min_hours": methodology.TEMP_TRUTH_MIN_HOURS,
        "temp_peak_window_high": list(methodology.TEMP_PEAK_WINDOW_HIGH),
        "temp_peak_window_low": list(methodology.TEMP_PEAK_WINDOW_LOW),
        "near_complete_slot_allowance": methodology.NEAR_COMPLETE_SLOT_ALLOWANCE,
        "roster_availability_floor": methodology.ROSTER_AVAILABILITY_FLOOR,
        "adequate_lead_min_days": methodology.ADEQUATE_LEAD_MIN_DAYS,
        "min_adequate_leads_per_variable": (
            methodology.MIN_ADEQUATE_LEADS_PER_VARIABLE
        ),
        "bootstrap_method": "moving_block_over_target_dates",
        "bootstrap_block_length_days": methodology.BOOTSTRAP_BLOCK_LENGTH_DAYS,
        "bootstrap_resamples": methodology.BOOTSTRAP_RESAMPLES,
        "candidate_ci_level": methodology.CANDIDATE_CI_LEVEL,
        "precip_improvement_ci_level": methodology.PRECIP_IMPROVEMENT_CI_LEVEL,
        "baseline_gate_ci_level": methodology.BASELINE_GATE_CI_LEVEL,
        "lead_stability_numerator": methodology.LEAD_STABILITY_NUMERATOR,
        "lead_stability_denominator": methodology.LEAD_STABILITY_DENOMINATOR,
        "practical_floor_relative_mae": methodology.PRACTICAL_FLOOR_RELATIVE_MAE,
        "practical_floor_ets": methodology.PRACTICAL_FLOOR_ETS,
        "non_inferiority_mae_margin": methodology.NON_INFERIORITY_MAE_MARGIN,
        "non_inferiority_ets_margin": methodology.NON_INFERIORITY_ETS_MARGIN,
        "occurrence_min_wet_days": methodology.OCCURRENCE_MIN_WET_DAYS,
        "occurrence_min_dry_days": methodology.OCCURRENCE_MIN_DRY_DAYS,
        "daily_rank_min_history_days": methodology.DAILY_RANK_MIN_HISTORY_DAYS,
        "simulated_depths": list(SIM_DEPTHS),
        "simulated_variables": list(SIM_VARIABLES),
    }
