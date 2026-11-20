"""
Resolves relative time period specs from the ground truth dataset into concrete filter values.

A case can replace hardcoded time period fields in ``expected_output`` with a ``$time`` object so that queries like
"last quarter" or "this fiscal year" stay correct no matter when the evaluation runs:

    "$time": {"unit": "fiscal_quarter", "offset": -1}

Spec keys:
    unit:    One of ``fiscal_year``, ``fiscal_quarter``, ``calendar_year`` or ``month``. (required)
    offset:  Whole units relative to the unit containing the as-of date; 0 is current, -1 is previous. (default 0)
    span:    Number of consecutive units, ending at ``offset``. (default 1)
    to_date: Cap the end of a date range at the as-of date, e.g. "so far this year". (default false)
    as:      ``fy`` or ``dr`` to force the output form. Defaults to ``fy`` for ``fiscal_year`` and ``dr`` otherwise.

Federal fiscal years are named for the calendar year they end in and run October 1 through September 30.
"""

import calendar
from datetime import date
from typing import Any

from usaspending_api.llm.evals.exceptions import DatasetError

TIME_KEY = "$time"

UNITS = {"fiscal_year", "fiscal_quarter", "calendar_year", "month"}
OUTPUT_FORMS = {"fy", "dr"}
SPEC_KEYS = {"unit", "offset", "span", "to_date", "as"}

# First month of each fiscal quarter, Q1 through Q4.
FISCAL_QUARTER_START_MONTHS = (10, 1, 4, 7)


def fiscal_year_of(day: date) -> int:
    return day.year + 1 if day.month >= 10 else day.year


def _fiscal_quarter_index(day: date) -> int:
    """Return a continuous index (fiscal year * 4 + zero-based quarter) for the quarter containing ``day``."""
    return fiscal_year_of(day) * 4 + ((day.month - 10) % 12) // 3


def _fiscal_quarter_start(index: int) -> date:
    fiscal_year, quarter = divmod(index, 4)
    month = FISCAL_QUARTER_START_MONTHS[quarter]
    return date(fiscal_year - 1 if quarter == 0 else fiscal_year, month, 1)


def _month_start(index: int) -> date:
    """``index`` is year * 12 + zero-based month."""
    year, month = divmod(index, 12)
    return date(year, month + 1, 1)


def _month_end(index: int) -> date:
    year, month = divmod(index, 12)
    return date(year, month + 1, calendar.monthrange(year, month + 1)[1])


def _date_range(unit: str, first: int, last: int) -> tuple[date, date]:
    """Return the start and end dates covering units ``first`` through ``last`` (inclusive, unit-specific index)."""
    result = None
    if unit == "fiscal_year":
        result = date(first - 1, 10, 1), date(last, 9, 30)
    if unit == "calendar_year":
        result = date(first, 1, 1), date(last, 12, 31)
    if unit == "fiscal_quarter":
        result = _fiscal_quarter_start(first), _fiscal_quarter_start(last + 1) - date.resolution
    return result or (_month_start(first), _month_end(last))


def _current_index(unit: str, today: date) -> int:
    result = None
    if unit == "fiscal_year":
        result = fiscal_year_of(today)
    if unit == "calendar_year":
        result = today.year
    if unit == "fiscal_quarter":
        result = _fiscal_quarter_index(today)
    return result or (today.year * 12 + today.month - 1)


def _validate(spec: Any, case_id: str) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} must be an object.")

    unknown_keys = set(spec) - SPEC_KEYS
    if unknown_keys:
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} has unknown key(s): {', '.join(sorted(unknown_keys))}.")

    if spec.get("unit") not in UNITS:
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} unit must be one of: {', '.join(sorted(UNITS))}.")

    for key in ("offset", "span"):
        value = spec.get(key, 0 if key == "offset" else 1)
        if isinstance(value, bool) or not isinstance(value, int):
            raise DatasetError(f"Case '{case_id}' {TIME_KEY} {key} must be an integer.")

    if spec.get("span", 1) < 1:
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} span must be at least 1.")

    if "as" in spec and spec["as"] not in OUTPUT_FORMS:
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} as must be one of: {', '.join(sorted(OUTPUT_FORMS))}.")

    if spec.get("as") == "fy" and spec["unit"] != "fiscal_year":
        raise DatasetError(f"Case '{case_id}' {TIME_KEY} can only output 'fy' for the fiscal_year unit.")

    return spec


def resolve_time_spec(spec: Any, today: date, case_id: str = "?") -> dict[str, Any]:
    """Convert a ``$time`` spec into ``timePeriodType`` plus ``timePeriodFY`` or ``time_period`` filter values."""
    spec = _validate(spec, case_id)
    unit = spec["unit"]
    last = _current_index(unit, today) + spec.get("offset", 0)
    first = last - spec.get("span", 1) + 1
    output_form = spec.get("as", "fy" if unit == "fiscal_year" else "dr")

    if output_form == "fy":
        return {"timePeriodType": "fy", "timePeriodFY": [str(year) for year in range(first, last + 1)]}

    start_date, end_date = _date_range(unit, first, last)
    if spec.get("to_date"):
        end_date = min(end_date, today)

    return {
        "timePeriodType": "dr",
        "time_period": [{"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}],
    }
