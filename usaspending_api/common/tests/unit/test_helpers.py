# Stdlib imports
from datetime import datetime

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel
from django_mock_queries.query import MockSet
import pytest

# Imports from your apps
from usaspending_api.common.helpers.generic_helper import check_valid_toptier_agency
from usaspending_api.common.helpers.generic_helper import generate_fiscal_period
from usaspending_api.common.helpers.generic_helper import generate_fiscal_year


# example of a mocked unit test
def test_check_valid_toptier_agency_valid(monkeypatch):
    agencies = MockSet()
    monkeypatch.setattr("usaspending_api.references.models.Agency.objects", agencies)
    agencies.add(MockModel(mock_name="toptier agency", id=12345, toptier_flag=True))
    assert check_valid_toptier_agency(12345)


def test_check_valid_toptier_agency_invalid(monkeypatch):
    agencies = MockSet()
    monkeypatch.setattr("usaspending_api.references.models.Agency.objects", agencies)
    agencies.add(MockModel(mock_name="subtier agency", id=54321, toptier_flag=False))
    assert not check_valid_toptier_agency(54321)


def test_generate_fiscal_period_beginning_of_fiscal_year():
    date = datetime.strptime("10/01/2018", "%m/%d/%Y")
    expected = 1
    actual = generate_fiscal_period(date)
    assert actual == expected


def test_generate_fiscal_period_end_of_fiscal_year():
    date = datetime.strptime("09/30/2019", "%m/%d/%Y")
    expected = 12
    actual = generate_fiscal_period(date)
    assert actual == expected


def test_generate_fiscal_period_middle_of_fiscal_year():
    date = datetime.strptime("01/01/2019", "%m/%d/%Y")
    expected = 4
    actual = generate_fiscal_period(date)
    assert actual == expected


def test_generate_fiscal_period_incorrect_data_type_string():
    with pytest.raises(TypeError):
        generate_fiscal_period("2019")


def test_generate_fiscal_period_incorrect_data_type_int():
    with pytest.raises(TypeError):
        generate_fiscal_period(2019)


def test_generate_fiscal_period_malformed_date_month_year():
    date = datetime.strptime("10/2018", "%m/%Y").date
    with pytest.raises(Exception):
        generate_fiscal_period(date)


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
