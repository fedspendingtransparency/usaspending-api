from unittest.mock import MagicMock, patch

from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import MonthlyType
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import MonthlyDownloadFilters
from usaspending_api.download.delta_downloads.transaction_assistance_monthly import (
    TransactionAssistanceMonthlyDownloadFactory,
)
from usaspending_api.download.delta_downloads.transaction_contract_monthly import (
    TransactionContractMonthlyDownloadFactory,
)


@patch(
    "usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory"
    ".AbstractMonthlyDownloadFactory.dynamic_filters"
)
def test_monthly_delta_fails_with_fiscal_year(mock_dynamic_filters):
    mock_spark = MagicMock()
    filters = MonthlyDownloadFilters(award_category="assistance", fiscal_year=2020)
    factory = TransactionAssistanceMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.DELTA)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'DELTA'" in str(err)
    else:
        raise AssertionError("No exception was raised")

    filters = MonthlyDownloadFilters(award_category="contract", fiscal_year=2020)
    factory = TransactionContractMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.DELTA)
    except ValueError as err:
        assert "'fiscal_year' is not supported for monthly_type of 'DELTA'" in str(err)
    else:
        raise AssertionError("No exception was raised")


@patch(
    "usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory"
    ".AbstractMonthlyDownloadFactory.dynamic_filters"
)
def test_monthly_full_fails_without_fiscal_year(mock_dynamic_filters):
    mock_spark = MagicMock()

    filters = MonthlyDownloadFilters(award_category="assistance")
    factory = TransactionAssistanceMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.FULL)
    except ValueError as err:
        assert "'fiscal_year' is required for monthly_type of 'FULL'" in str(err)
    else:
        raise AssertionError("No exception was raised")

    filters = MonthlyDownloadFilters(award_category="contract")
    factory = TransactionContractMonthlyDownloadFactory(mock_spark, filters)
    try:
        factory.get_download(MonthlyType.FULL)
    except ValueError as err:
        assert "'fiscal_year' is required for monthly_type of 'FULL'" in str(err)
    else:
        raise AssertionError("No exception was raised")
