from unittest.mock import patch

import pandas as pd
import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import MonthlyType
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import MonthlyDownloadFilters
from usaspending_api.download.delta_downloads.transaction_assistance_monthly import (
    TransactionAssistanceMonthlyDownloadFactory,
)
from usaspending_api.download.delta_downloads.transaction_contract_monthly import (
    TransactionContractMonthlyDownloadFactory,
)
from usaspending_api.download.delta_models.transaction_download import transaction_download_schema


@pytest.fixture
def agency_models(db):
    baker.make("references.ToptierAgency", pk=1, toptier_code="097", abbreviation="DOD")
    baker.make("references.ToptierAgency", pk=2, toptier_code="012", abbreviation="USDA")


@pytest.fixture
def transaction_download_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        "--destination-table=transaction_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    column_placeholders = {field.name: [None] * 6 for field in transaction_download_schema}
    test_data_df = pd.DataFrame(
        data={
            **column_placeholders,
            "transaction_unique_key": ["ASST_TX_1", "ASST_TX_2", "ASST_TX_3", "CONT_TX_1", "CONT_TX_2", "CONT_TX_3"],
            "is_fpds": [False, False, False, True, True, True],
            "award_id": [1, 2, 2, 3, 4, 4],
            "generated_unique_award_id": [
                "ASST_AWD_1",
                "ASST_AWD_2",
                "ASST_AWD_2",
                "CONT_AWD_1",
                "CONT_AWD_2",
                "CONT_AWD_2",
            ],
            "usaspending_permalink": ["some_link", "some_link", "some_link", "some_link", "some_link", "some_link"],
            "transaction_id": [1, 2, 3, 4, 5, 6],
            "transaction_broker_id": [100, 200, 300, 400, 500, 600],
            "action_date_fiscal_year": [2020, 2021, 2021, 2020, 2021, 2021],
            "awarding_agency_code": ["097", "097", "012", "012", "012", "097"],
        }
    )

    (
        spark.createDataFrame(test_data_df, schema=transaction_download_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("rpt.transaction_download")
    )
    yield


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.date")
def test_assistance_delta_monthly_download_factory(mock_date, spark, transaction_download_table, agency_models):
    mock_date.today.return_value.strftime.return_value = "20210130"
    download_filters = MonthlyDownloadFilters(
        awarding_toptier_agency_abbreviation="DOD",
    )
    factory = TransactionAssistanceMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.DELTA)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 2
    assert result.file_names == ["FY(All)_097_Assistance_Delta_20210130"]
    assert sorted(result_df.assistance_transaction_unique_key.to_list()) == ["ASST_TX_1", "ASST_TX_2"]

    download_filters = MonthlyDownloadFilters(as_of_date="20250130")
    factory = TransactionAssistanceMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.DELTA)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 3
    assert result.file_names == ["FY(All)_All_Assistance_Delta_20250130"]
    assert sorted(result_df.assistance_transaction_unique_key.to_list()) == ["ASST_TX_1", "ASST_TX_2", "ASST_TX_3"]


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.date")
def test_assistance_full_monthly_download_factory(mock_date, spark, transaction_download_table, agency_models):
    mock_date.today.return_value.strftime.return_value = "20210130"
    download_filters = MonthlyDownloadFilters(awarding_toptier_agency_abbreviation="DOD", fiscal_year="2020")
    factory = TransactionAssistanceMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.FULL)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 1
    assert result.file_names == ["FY2020_097_Assistance_Full_20210130"]
    assert sorted(result_df.assistance_transaction_unique_key.to_list()) == ["ASST_TX_1"]

    download_filters = MonthlyDownloadFilters(as_of_date="20250130", fiscal_year="2021")
    factory = TransactionAssistanceMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.FULL)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 2
    assert result.file_names == ["FY2021_All_Assistance_Full_20250130"]
    assert sorted(result_df.assistance_transaction_unique_key.to_list()) == ["ASST_TX_2", "ASST_TX_3"]


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.date")
def test_contract_delta_monthly_download_factory(mock_date, spark, transaction_download_table, agency_models):
    mock_date.today.return_value.strftime.return_value = "20210130"
    download_filters = MonthlyDownloadFilters(
        awarding_toptier_agency_abbreviation="USDA",
    )
    factory = TransactionContractMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.DELTA)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 2
    assert result.file_names == ["FY(All)_012_Contracts_Delta_20210130"]
    assert sorted(result_df.contract_transaction_unique_key.to_list()) == ["CONT_TX_1", "CONT_TX_2"]

    download_filters = MonthlyDownloadFilters(as_of_date="20250130")
    factory = TransactionContractMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.DELTA)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 3
    assert result.file_names == ["FY(All)_All_Contracts_Delta_20250130"]
    assert sorted(result_df.contract_transaction_unique_key.to_list()) == ["CONT_TX_1", "CONT_TX_2", "CONT_TX_3"]


@patch("usaspending_api.download.delta_downloads.filters.monthly_download_filters.date")
def test_contract_full_monthly_download_factory(mock_date, spark, transaction_download_table, agency_models):
    mock_date.today.return_value.strftime.return_value = "20210130"
    download_filters = MonthlyDownloadFilters(awarding_toptier_agency_abbreviation="USDA", fiscal_year="2020")
    factory = TransactionContractMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.FULL)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 1
    assert result.file_names == ["FY2020_012_Contracts_Full_20210130"]
    assert sorted(result_df.contract_transaction_unique_key.to_list()) == ["CONT_TX_1"]

    download_filters = MonthlyDownloadFilters(as_of_date="20250130", fiscal_year="2021")
    factory = TransactionContractMonthlyDownloadFactory(spark, download_filters)
    result = factory.get_download(MonthlyType.FULL)
    result_df = result.dataframes[0].toPandas()

    assert result.dataframes[0].count() == 2
    assert result.file_names == ["FY2021_All_Contracts_Full_20250130"]
    assert sorted(result_df.contract_transaction_unique_key.to_list()) == ["CONT_TX_2", "CONT_TX_3"]
