from datetime import datetime, timezone
from django.db.models import F

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


def get_dabs_schedule(is_quarter):
    return list(
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=is_quarter, submission_reveal_date__lte=datetime.now(timezone.utc)
        )
        .annotate(month=F("submission_fiscal_month"), year=F("submission_fiscal_year"))
        .order_by("-submission_fiscal_year", "-submission_fiscal_month")
        .values("month", "year", "is_quarter", "submission_reveal_date")
    )


def retrieve_recent_periods():
    recent_month_periods = get_dabs_schedule(False)
    recent_quarter_periods = get_dabs_schedule(True)

    recent_periods = {
        "this_month": recent_month_periods[0],
        "this_quarter": recent_quarter_periods[0],
        "last_month": recent_month_periods[1],
        "last_quarter": recent_quarter_periods[1],
    }

    return recent_periods
