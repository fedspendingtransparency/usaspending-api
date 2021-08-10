from dataclasses import dataclass
from django.db import connection
from django.db.models import Q, Max
from typing import Optional, List

from django_cte import With

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import is_final_quarter, is_final_period
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule, SubmissionAttributes


def get_last_closed_submission_date(is_quarter: Optional[bool] = None) -> Optional[dict]:
    filters = {"submission_reveal_date__lte": now()}
    if is_quarter is not None:
        filters["is_quarter"] = is_quarter
    return (
        DABSSubmissionWindowSchedule.objects.filter(**filters)
        .order_by("-submission_fiscal_year", "-submission_fiscal_quarter", "-submission_fiscal_month")
        .values()
        .first()
    )


def validate_request_within_revealed_submissions(
    fiscal_year: int,
    fiscal_quarter: Optional[int] = None,
    fiscal_period: Optional[int] = None,
    is_quarter: Optional[bool] = None,
) -> None:

    latest_submission_period = get_last_closed_submission_date(is_quarter=is_quarter)
    sub_window_year = latest_submission_period["submission_fiscal_year"]
    sub_window_quarter = latest_submission_period["submission_fiscal_quarter"]
    sub_window_period = latest_submission_period["submission_fiscal_month"]

    invalid_submission_date_range = False
    msg = "Value for {filter} is outside the range of current submissions"

    if fiscal_year > sub_window_year:
        invalid_submission_date_range = True
        msg = msg.format(filter="fiscal_year")
    elif fiscal_year == sub_window_year:
        if fiscal_quarter and fiscal_quarter > sub_window_quarter:
            invalid_submission_date_range = True
            msg = msg.format(filter="fiscal_quarter")
        elif fiscal_period and fiscal_period > sub_window_period:
            invalid_submission_date_range = True
            msg = msg.format(filter="fiscal_period")

    if invalid_submission_date_range:
        raise InvalidParameterException(msg)


def is_valid_monthly_period(year: int, period: int) -> bool:
    """ Returns False for periods before agencies were able to make monthly submissions """

    is_valid_period = True

    if year == 2020 and period in [2, 4, 5]:
        is_valid_period = False
    if year < 2020 and period in [2, 4, 5, 7, 8, 10, 11]:
        is_valid_period = False
    if year == 2017 and period == 3:
        is_valid_period = False
    if period == 1:
        is_valid_period = False

    return is_valid_period


@dataclass
class ClosedPeriod:
    """ Little convenience class to bundle some common period functionality. """

    fiscal_year: int
    fiscal_quarter: Optional[int]
    fiscal_month: Optional[int]

    def __post_init__(self):
        if self.fiscal_quarter is None and self.fiscal_month is None:
            raise RuntimeError("At least one of fiscal_quarter or fiscal_month is required.")

    @property
    def is_final(self) -> bool:
        return (self.fiscal_quarter is None or is_final_quarter(self.fiscal_quarter)) and (
            self.fiscal_month is None or is_final_period(self.fiscal_month)
        )

    def build_period_q(self, submission_relation_name: Optional[str] = None) -> Q:
        """ Leave submission_relation_name None to filter directly on the submission table. """
        prefix = f"{submission_relation_name}__" if submission_relation_name else ""
        q = Q()
        if self.fiscal_quarter:
            q |= Q(**{f"{prefix}reporting_fiscal_quarter": self.fiscal_quarter, f"{prefix}quarter_format_flag": True})
        if self.fiscal_month:
            q |= Q(**{f"{prefix}reporting_fiscal_period": self.fiscal_month, f"{prefix}quarter_format_flag": False})
        return Q(Q(q) & Q(**{f"{prefix}reporting_fiscal_year": self.fiscal_year}))

    def build_submission_id_q(self, submission_relation_name: Optional[str] = None) -> Q:
        prefix = f"{submission_relation_name}__" if submission_relation_name else ""
        submission_ids = get_submission_ids_for_periods(self.fiscal_year, self.fiscal_quarter, self.fiscal_month)
        if not submission_ids:
            # If there are no submission ids it means there are no submissions in the period which
            # means nothing should be returned.
            return Q(**{f"{prefix}submission_id__isnull": True})
        return Q(**{f"{prefix}submission_id__in": submission_ids})


def get_last_closed_quarter_relative_to_month(fiscal_year: int, fiscal_month: int) -> Optional[dict]:
    """ Returns the most recently closed fiscal quarter in the fiscal year less than or equal to the fiscal month. """
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=True,
            submission_fiscal_year=fiscal_year,
            submission_fiscal_month__lte=fiscal_month,
            submission_reveal_date__lte=now(),
        )
        .order_by("-submission_fiscal_quarter")
        .values_list("submission_fiscal_quarter", flat=True)
        .first()
    )


def get_last_closed_month_relative_to_quarter(fiscal_year: int, fiscal_quarter: int) -> Optional[dict]:
    """ Returns the most recently closed fiscal month in the fiscal year less than or equal to the fiscal quarter. """
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            is_quarter=False,
            submission_fiscal_year=fiscal_year,
            submission_fiscal_quarter__lte=fiscal_quarter,
            submission_reveal_date__lte=now(),
        )
        .order_by("-submission_fiscal_month")
        .values_list("submission_fiscal_month", flat=True)
        .first()
    )


def get_submission_ids_for_periods(
    fiscal_year: int, fiscal_quarter: Optional[int], fiscal_month: Optional[int]
) -> List[int]:
    """
    Find quarterly submissions that match the quarter filter and monthly submissions that match the
    monthly filter.  The catch is that we need to account for agencies that fall in both.  For
    example, if DOT submitted for Q2 and P07, we only care about their P07 submissions.  This can
    happen when agencies first transition from quarterly submissions to monthly submissions.
    """
    sql = f"""
        select  submission_id
        from    submission_attributes
        where   (toptier_code, reporting_fiscal_year, reporting_fiscal_period) in (
                    select  distinct on (toptier_code)
                            toptier_code, reporting_fiscal_year, reporting_fiscal_period
                    from    submission_attributes
                    where   reporting_fiscal_year = %(fiscal_year)s and
                            (
                                (reporting_fiscal_quarter <= %(fiscal_quarter)s and quarter_format_flag is true) or
                                (reporting_fiscal_period <= %(fiscal_month)s and quarter_format_flag is false)
                            )
                    order   by toptier_code, reporting_fiscal_period desc
                ) and
                (
                    (reporting_fiscal_quarter = %(fiscal_quarter)s and quarter_format_flag is true) or
                    (reporting_fiscal_period = %(fiscal_month)s and quarter_format_flag is false)
                )
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            {"fiscal_year": fiscal_year, "fiscal_quarter": fiscal_quarter or -1, "fiscal_month": fiscal_month or -1},
        )
        return [r[0] for r in cursor.fetchall()]


def get_latest_submission_ids_for_fiscal_year(fiscal_year: int):
    """
    Returns a list of submission_ids that consists of the latest submission_id for each Reporting Agency.
    This list will capture cases where a Reporting Agency might not submit in the most recent Submission Period
    but they do have a Submission in the provided Fiscal Year.
    """
    cte = With(
        SubmissionAttributes.objects.filter(
            submission_window__submission_reveal_date__lte=now(), reporting_fiscal_year=fiscal_year
        )
        .values("toptier_code")
        .annotate(latest_fiscal_period=Max("reporting_fiscal_period"))
    )
    submission_ids = list(
        cte.join(
            SubmissionAttributes,
            toptier_code=cte.col.toptier_code,
            reporting_fiscal_period=cte.col.latest_fiscal_period,
            reporting_fiscal_year=fiscal_year,
        )
        .with_cte(cte)
        .values_list("submission_id", flat=True)
    )
    return submission_ids


def get_latest_submission_ids_for_each_fiscal_quarter(
    is_file_a, fiscal_years: List[int] = [], federal_account_id: int = None
):
    """
    Returns a list of submission_ids that consists of the latest submission_id for each quarter of a given
    fiscal year and federal account. This list will capture cases where a Reporting Agency might not submit
    in the most recent Submission Period but they do have a Submission in the provided Fiscal Year.
    """

    if is_file_a:
        federal_account_id_filter_obj = "appropriation_account_balances"
    else:
        federal_account_id_filter_obj = "financial_accounts_by_program_activity_object_classes"

    filters = {"submission_window__submission_reveal_date__lte": now()}
    if len(fiscal_years) > 0:
        filters["reporting_fiscal_year__in"] = fiscal_years
    if federal_account_id:
        filters[f"{federal_account_id_filter_obj}__treasury_account__federal_account_id"] = federal_account_id

    cte = With(
        SubmissionAttributes.objects.filter(**filters)
        .values("toptier_code", "reporting_fiscal_year", "reporting_fiscal_quarter")
        .annotate(latest_fiscal_period=Max("reporting_fiscal_period"))
    )
    submission_ids = list(
        cte.join(
            SubmissionAttributes,
            toptier_code=cte.col.toptier_code,
            reporting_fiscal_period=cte.col.latest_fiscal_period,
        )
        .with_cte(cte)
        .values_list("submission_id", flat=True)
    )
    return submission_ids
