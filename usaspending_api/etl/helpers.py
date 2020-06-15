import warnings

from django.core.cache import CacheKeyWarning


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
