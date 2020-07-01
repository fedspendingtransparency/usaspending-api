from datetime import datetime, timezone
from typing import Optional
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


def get_last_closed_submission_date(is_quarter: bool) -> Optional[dict]:
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=is_quarter, submission_reveal_date__lte=datetime.now(timezone.utc)
        )
        .order_by("-submission_fiscal_year", "-submission_fiscal_quarter", "-submission_fiscal_month")
        .values()
        .first()
    )


def get_last_closed_quarter_relative_to_period(fiscal_year: int, fiscal_period: int) -> Optional[dict]:
    """ Returns the mostly recently closed quarter in the fiscal year less than or equal to the period provided. """
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=True,
            submission_fiscal_year=fiscal_year,
            submission_fiscal_month__lte=fiscal_period,
            submission_reveal_date__lte=datetime.now(timezone.utc),
        )
        .order_by("-submission_fiscal_quarter")
        .values_list("submission_fiscal_quarter", flat=True)
        .first()
    )


def get_last_closed_period_relative_to_quarter(fiscal_year: int, fiscal_quarter: int) -> Optional[dict]:
    """ Returns the mostly recently closed period in the fiscal year less than or equal to the quarter provided. """
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=False,
            submission_fiscal_year=fiscal_year,
            submission_fiscal_quarter__lte=fiscal_quarter,
            submission_reveal_date__lte=datetime.now(timezone.utc),
        )
        .order_by("-submission_fiscal_month")
        .values_list("submission_fiscal_month", flat=True)
        .first()
    )
