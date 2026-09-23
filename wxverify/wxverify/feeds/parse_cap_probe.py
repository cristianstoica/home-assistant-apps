"""Temporary parse-cap probe for the Visual Crossing, Meteosource and Meteoblue
adapters.

Re-parses a response the adapter already holds with the lead-hour cap lifted
and logs, per model, how far its usable hours reach past the cap. A record
holds only the source literal, an allowlisted model name, the request's
variable names, integer counts and lead hours, ``none``, or, on failure, an
exception class name. The re-parse is discarded once summarized, so nothing
it produces is returned or stored.
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from itertools import pairwise
from typing import Final

from wxverify.core.timeutil import parse_utc
from wxverify.feeds.seam import FetchResult, ForecastRequest, NormalizedSample

_MODEL_NAME_ALLOWLIST: Final = r"[A-Za-z0-9_.-]{1,64}"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VariableCoverage:
    variable: str
    uncapped: int
    capped: int
    max_lead: int | None


@dataclass(frozen=True)
class ParseCapSummary:
    model: str
    cap: int
    uncapped: int
    capped: int
    min_lead: int | None
    max_lead: int | None
    gaps: int
    gap_hours: int
    variables: tuple[VariableCoverage, ...]


def summarize_parse_cap(
    samples: Sequence[NormalizedSample],
    *,
    models: Sequence[str],
    variables: Sequence[str],
    cap: int,
) -> list[ParseCapSummary]:
    """Summarize uncapped coverage per distinct model name, in first-seen order.

    Counts are of distinct ``valid_at`` instants, not of samples. A model with
    no usable sample still gets a zeroed summary.
    """
    summaries: list[ParseCapSummary] = []
    for model in dict.fromkeys(models):
        usable = [
            sample
            for sample in samples
            if sample.model == model and sample.variable in variables
        ]
        uncapped, capped = _instant_counts(usable, cap)
        gaps, gap_hours = _gap_counts(usable)
        summaries.append(
            ParseCapSummary(
                model=model,
                cap=cap,
                uncapped=uncapped,
                capped=capped,
                min_lead=min((sample.lead_hours for sample in usable), default=None),
                max_lead=max((sample.lead_hours for sample in usable), default=None),
                gaps=gaps,
                gap_hours=gap_hours,
                variables=tuple(
                    _variable_coverage(variable, usable, cap) for variable in variables
                ),
            )
        )
    return summaries


def _variable_coverage(
    variable: str, usable: Sequence[NormalizedSample], cap: int
) -> VariableCoverage:
    subset = [sample for sample in usable if sample.variable == variable]
    uncapped, capped = _instant_counts(subset, cap)
    return VariableCoverage(
        variable=variable,
        uncapped=uncapped,
        capped=capped,
        max_lead=max((sample.lead_hours for sample in subset), default=None),
    )


def _instant_counts(samples: Sequence[NormalizedSample], cap: int) -> tuple[int, int]:
    uncapped = {parse_utc(sample.valid_at) for sample in samples}
    capped = {
        parse_utc(sample.valid_at) for sample in samples if sample.lead_hours <= cap
    }
    return len(uncapped), len(capped)


def _gap_counts(samples: Sequence[NormalizedSample]) -> tuple[int, int]:
    instants = sorted({parse_utc(sample.valid_at) for sample in samples})
    gaps = 0
    gap_hours = 0
    for earlier, later in pairwise(instants):
        delta = later - earlier
        if delta > timedelta(hours=1):
            gaps += 1
            whole, rest = divmod(delta, timedelta(hours=1))
            gap_hours += whole - 1 + (1 if rest else 0)
    return gaps, gap_hours


def format_parse_cap_record(source: str, index: int, summary: ParseCapSummary) -> str:
    """Render one ``status=ok`` record.

    A model name outside the allowlist is printed as ``member<index>``, and
    every missing lead as ``none``.
    """
    model = (
        summary.model
        if re.fullmatch(_MODEL_NAME_ALLOWLIST, summary.model)
        else f"member{index}"
    )
    fields = [
        "parse_cap_probe status=ok",
        f"source={source}",
        f"model={model}",
        f"cap={summary.cap}",
        f"uncapped={summary.uncapped}",
        f"capped={summary.capped}",
        f"min_lead={_lead_text(summary.min_lead)}",
        f"max_lead={_lead_text(summary.max_lead)}",
        f"gaps={summary.gaps}",
        f"gap_hours={summary.gap_hours}",
    ]
    for coverage in summary.variables:
        fields.extend(
            (
                f"{coverage.variable}_uncapped={coverage.uncapped}",
                f"{coverage.variable}_capped={coverage.capped}",
                f"{coverage.variable}_max_lead={_lead_text(coverage.max_lead)}",
            )
        )
    return " ".join(fields)


def _lead_text(value: int | None) -> str:
    return "none" if value is None else str(value)


def emit_parse_cap_probe(
    *,
    source: str,
    req: ForecastRequest,
    models: Sequence[str],
    reparse: Callable[[ForecastRequest], FetchResult],
) -> None:
    """Log one record per model for an uncapped re-parse of the same payload.

    Every record is built before any is written. Any failure is reduced to a
    single ``status=error`` record naming the exception class only.
    """
    try:
        shadow = reparse(req.model_copy(update={"max_lead_hours": sys.maxsize}))
        summaries = summarize_parse_cap(
            shadow.samples,
            models=models,
            variables=req.variables,
            cap=req.max_lead_hours,
        )
        lines = [
            format_parse_cap_record(source, index, summary)
            for index, summary in enumerate(summaries)
        ]
    except Exception as exc:
        logger.info(
            "parse_cap_probe status=error source=%s error_type=%s",
            source,
            type(exc).__name__,
        )
        return
    for line in lines:
        logger.info("%s", line)
