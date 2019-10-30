import warnings

from django.core.cache import CacheKeyWarning
from usaspending_api.submissions.models import SubmissionAttributes


warnings.simplefilter("ignore", CacheKeyWarning)


def get_fiscal_quarter(fiscal_reporting_period):
    """
    Return the fiscal quarter.
    Note: the reporting period being passed should already be in "federal fiscal format",
    where period 1 = Oct. and period 12 = Sept.
    """
    if fiscal_reporting_period in [1, 2, 3]:
        return 1
    elif fiscal_reporting_period in [4, 5, 6]:
        return 2
    elif fiscal_reporting_period in [7, 8, 9]:
        return 3
    elif fiscal_reporting_period in [10, 11, 12]:
        return 4


def get_previous_submission(toptier_code, fiscal_year, fiscal_period):
    """
    For the specified CGAC/FREC (e.g., department/top-tier agency) and specified fiscal year and quarter, return
    the previous submission within the same fiscal year.
    """
    previous_submission = (
        SubmissionAttributes.objects.filter(
            toptier_code=toptier_code,
            reporting_fiscal_year=fiscal_year,
            reporting_fiscal_period__lt=fiscal_period,
            quarter_format_flag=True,
        )
        .order_by("-reporting_fiscal_period")
        .first()
    )
    return previous_submission
