import pytest
from datetime import datetime

from model_bakery import baker

from usaspending_api.common.helpers.generic_helper import check_valid_toptier_agency
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_month, generate_fiscal_year


@pytest.mark.django_db
def test_check_valid_toptier_agency_valid():
    baker.make("references.Agency", id=12345, toptier_flag=True)
    assert check_valid_toptier_agency(12345)


@pytest.mark.django_db
def test_check_valid_toptier_agency_invalid():
    baker.make("references.Agency", id=54321, toptier_flag=False)
    assert not check_valid_toptier_agency(54321)


def test_generate_fiscal_period_beginning_of_fiscal_year():
    date = datetime.strptime("10/01/2018", "%m/%d/%Y")
    expected = 1
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_end_of_fiscal_year():
    date = datetime.strptime("09/30/2019", "%m/%d/%Y")
    expected = 12
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_middle_of_fiscal_year():
    date = datetime.strptime("01/01/2019", "%m/%d/%Y")
    expected = 4
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_incorrect_data_type_string():
    with pytest.raises(TypeError):
        generate_fiscal_month("2019")


def test_generate_fiscal_period_incorrect_data_type_int():
    with pytest.raises(TypeError):
        generate_fiscal_month(2019)


def test_generate_fiscal_period_malformed_date_month_year():
    date = datetime.strptime("10/2018", "%m/%Y").date
    with pytest.raises(Exception):
        generate_fiscal_month(date)


# example of a simple unit test
def test_beginning_of_fiscal_year():
    date = datetime.strptime("10/01/2018", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_end_of_fiscal_year():
    date = datetime.strptime("09/30/2019", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_middle_of_fiscal_year():
    date = datetime.strptime("01/01/2019", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_incorrect_data_type_string():
    with pytest.raises(TypeError):
        generate_fiscal_year("2019")


def test_incorrect_data_type_int():
    with pytest.raises(TypeError):
        generate_fiscal_year(2019)


def test_malformed_date_month_year():
    date = datetime.strptime("10/2018", "%m/%Y").date
    with pytest.raises(Exception):
        generate_fiscal_year(date)
