from datetime import date
from unittest.mock import patch

from pydantic import ValidationError

from usaspending_api.download.delta_downloads.filters.monthly_download_filters import (
    MonthlyDownloadFilters,
)


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_valid_fiscal_year(mock_toptier_agency):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = True
    try:
        MonthlyDownloadFilters(
            awarding_toptier_agency_code="097",
            fiscal_year=2000,
        )
    except ValidationError as err:
        assert len(err.raw_errors) == 1
        assert "Fiscal year of '2000' is below the minimum of 2008" in str(err.raw_errors[0])
    else:
        raise AssertionError("No exception was raised")


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_toptier_agency_code_error(mock_toptier_agency):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = False
    try:
        MonthlyDownloadFilters(
            awarding_toptier_agency_code="012",
            fiscal_year=2008,
        )
    except ValidationError as err:
        assert len(err.raw_errors) == 1
        assert "Invalid toptier code for 'awarding_toptier_agency_code': 012" in str(err.raw_errors[0])
    else:
        raise AssertionError("No exception was raised")


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_incorrect_format_as_of_date(mock_toptier_agency):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = True
    try:
        MonthlyDownloadFilters(
            awarding_toptier_agency_code="097",
            as_of_date="123",
        )
    except ValidationError as err:
        assert len(err.raw_errors) == 1
        assert "'as_of_date' must be in the format yyyyMMdd" in str(err.raw_errors[0])
    else:
        raise AssertionError("No exception was raised")


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.date")
@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_default_as_of_date(mock_toptier_agency, mock_date):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = True
    mock_date.today.return_value.strftime.return_value = "20210130"

    filters = MonthlyDownloadFilters(awarding_toptier_agency_code="097")

    assert filters.as_of_date == "20210130"


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_correct_format_as_of_date(mock_toptier_agency):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = True

    filters = MonthlyDownloadFilters(
        awarding_toptier_agency_code="097",
        as_of_date="20210130",
    )

    assert filters.as_of_date == "20210130"


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.ToptierAgency")
def test_wrong_type_as_of_date(mock_toptier_agency):
    mock_toptier_agency.objects.filter.return_value.exists.return_value = True
    as_of_date = date(2020, 1, 30)
    try:
        MonthlyDownloadFilters(
            awarding_toptier_agency_code="097",
            as_of_date=as_of_date,
        )
    except ValidationError as err:
        assert len(err.raw_errors) == 1
        assert f"Received unsupported type of '{type(as_of_date)}'; expected 'str'" in str(err.raw_errors[0])
    else:
        raise AssertionError("No exception was raised")
