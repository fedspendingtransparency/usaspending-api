from datetime import datetime, timezone

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


def get_last_closed_submission_date(is_quarter: bool) -> dict:
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=is_quarter, submission_reveal_date__lte=datetime.now(timezone.utc)
        )
        .order_by("-submission_fiscal_year", "-submission_fiscal_quarter", "-submission_fiscal_month")
        .values()
        .first()
    )
