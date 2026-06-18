from unittest.mock import MagicMock, patch

import pytest

from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import MonthlyType
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import MonthlyDownloadFilters
from usaspending_api.download.delta_downloads.transaction_assistance_monthly import (
    TransactionAssistanceMonthlyDownloadFactory,
)
from usaspending_api.download.delta_downloads.transaction_contract_monthly import (
    TransactionContractMonthlyDownloadFactory,
)


@pytest.fixture
def setup_toptier_agency(db):
    """Create a test toptier agency with code 097"""
    from usaspending_api.references.models import ToptierAgency

    ToptierAgency.objects.get_or_create(
        toptier_agency_id=1,
        defaults={
            'toptier_code': '097',
            'name': 'Test Agency',
        }
    )


@pytest.mark.django_db
@patch(
    "usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory"
    ".AbstractMonthlyDownloadFactory.dynamic_filters"
)
def test_monthly_delta_fails_with_fiscal_year(mock_dynamic_filters, setup_toptier_agency):
    mock_spark = MagicMock()
    filters = MonthlyDownloadFilters(awarding_toptier_agency_code="097", fiscal_year=2020,
                                     delta_start_date="2020-01-01")
    factory = TransactionAssistanceMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.DELTA)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'DELTA'" in str(err)
    else:
        raise AssertionError("No exception was raised")

    filters = MonthlyDownloadFilters(awarding_toptier_agency_code="097", fiscal_year=2020,
                                     delta_start_date="2020-01-01")
    factory = TransactionContractMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.DELTA)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'DELTA'" in str(err)
    else:
        raise AssertionError("No exception was raised")


@pytest.mark.django_db
@patch(
    "usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory"
    ".AbstractMonthlyDownloadFactory.dynamic_filters"
)
def test_monthly_full_fails_with_fiscal_year(mock_dynamic_filters, setup_toptier_agency):
    mock_spark = MagicMock()

    filters = MonthlyDownloadFilters(awarding_toptier_agency_code="097", fiscal_year=2020)
    factory = TransactionAssistanceMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.FULL)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'FULL'" in str(err)
    else:
        raise AssertionError("No exception was raised")

    filters = MonthlyDownloadFilters(awarding_toptier_agency_code="097", fiscal_year=2020)
    factory = TransactionContractMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.FULL)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'FULL'" in str(err)
    else:
        raise AssertionError("No exception was raised")
