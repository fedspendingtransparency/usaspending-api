# Stdlib imports
import datetime
from dateutil.relativedelta import relativedelta

# Core Django imports
from django.conf import settings

# Third-party app imports
import pytest

# Imports from your apps
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters
from usaspending_api.common.exceptions import InvalidParameterException


def test_validate_year_success_digit():
    year = "2000"
    assert validate_year(year) == year


def test_validate_year_success_all():
    year = "all"
    assert validate_year(year) == year


def test_validate_year_success_latest():
    year = "latest"
    assert validate_year(year) == year


def test_validate_year_failure():
    year = "abc"

    with pytest.raises(InvalidParameterException):
        validate_year(year)


def test_reshape_filters_state():

    result = reshape_filters(state_code="AB")
    expected = {"country": "USA", "state": "AB"}

    assert result["place_of_performance_locations"][0] == expected


def test_reshape_filters_year_digit():
    year = "2017"
    result = reshape_filters(year=year)
    expected = {"start_date": "2016-10-01", "end_date": "2017-09-30"}

    assert result["time_period"][0] == expected


def test_reshape_filters_year_all():
    year = "all"
    result = reshape_filters(year=year)
    expected = {
        "start_date": settings.API_SEARCH_MIN_DATE,
        "end_date": datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
    }

    assert result["time_period"][0] == expected


def test_reshape_filters_year_latest():
    year = "latest"
    result = reshape_filters(year=year)
    expected = {
        "start_date": datetime.datetime.strftime(datetime.datetime.now() - relativedelta(years=1), "%Y-%m-%d"),
        "end_date": datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"),
    }

    assert result["time_period"][0] == expected


def test_reshape_filters_award_type_codes():
    award_type_codes = ["A", "B"]
    result = reshape_filters(award_type_codes=award_type_codes)

    assert result["award_type_codes"] == award_type_codes


def test_reshape_filters_duns():
    duns = "012345678"
    result = reshape_filters(duns_search_texts=duns)

    assert result["recipient_search_text"] == duns


def test_reshape_filters_recipient_id():
    recipient_id = "00000-fddfdbe-3fcsss5-9d252-d436c0ae8758c-R"
    result = reshape_filters(recipient_id=recipient_id)

    assert result["recipient_id"] == recipient_id
