from datetime import datetime, timezone

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


def get_last_closed_submission_date(is_quarter: bool) -> dict:
    submission = _dabs_objects(is_quarter).first()
    return {k: submission[k] for k in values(is_quarter)}


def get_last_closed_submissions_of_each_FY(is_quarter: bool):
    submissions = _dabs_objects(is_quarter)
    return [{k: submission[k] for k in values(is_quarter)} for submission in submissions]


def _dabs_objects(is_quarter: bool):
    current_date = datetime.now(timezone.utc).date()
    return (
        DABSSubmissionWindowSchedule.objects.filter(is_quarter=is_quarter, submission_reveal_date__lte=current_date)
        .values(*values(is_quarter))
        .order_by(*order_by(is_quarter))
    )


def values(is_quarter: bool):
    values = ["submission_fiscal_year"]
    if is_quarter:
        values.append("submission_fiscal_quarter")
    else:
        values.append("submission_fiscal_month")
    return values


def order_by(is_quarter: bool):
    order_by = ["-submission_fiscal_year"]
    if is_quarter:
        order_by.append("-submission_fiscal_quarter")
    else:
        order_by.append("-submission_fiscal_month")
    return order_by
