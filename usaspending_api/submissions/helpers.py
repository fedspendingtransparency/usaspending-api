from datetime import datetime

from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


def get_last_closed_submission_date(is_quarter: bool) -> dict:
    current_date = datetime.now().date()
    values = ["submission_fiscal_year"]
    order_by = ["-submission_fiscal_year"]
    if is_quarter:
        values.append("submission_fiscal_quarter")
        order_by.append("-submission_fiscal_quarter")
    else:
        values.append("submission_fiscal_month")
        order_by.append("-submission_fiscal_month")
    last_closed_submission = (
        DABSSubmissionWindowSchedule.objects.filter(is_quarter=is_quarter, submission_reveal_date__lte=current_date)
        .values(*values)
        .order_by(*order_by)
        .first()
    )
    return {k: last_closed_submission[k] for k in values}
