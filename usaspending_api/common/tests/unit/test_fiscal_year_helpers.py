from fiscalyear import FiscalDate

from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list


def test_all_fiscal_years():
    range_list = list(range(2001))
    assert range_list == create_fiscal_year_list(start_year=0, end_year=2001)


def test_current_fiscal_year():
    current_fiscal_year = FiscalDate.today().year
    fiscal_year_list = create_fiscal_year_list(start_year=2010)
    assert fiscal_year_list[0] == 2010
    assert fiscal_year_list[-1] == current_fiscal_year
