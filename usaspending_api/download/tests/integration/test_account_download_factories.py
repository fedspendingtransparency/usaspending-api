from unittest.mock import patch

import pandas as pd
import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.download.delta_downloads.account_balances import AccountBalancesDownloadFactory
from usaspending_api.download.delta_downloads.award_financial import AwardFinancialDownloadFactory
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.delta_models.account_balances_download import account_balances_schema
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


@pytest.fixture(scope="function")
def award_financial_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        f"--destination-table=award_financial_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    columns = list(
        set(
            [
                col
                for col in query_paths["award_financial"]["federal_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + [
                col
                for col in query_paths["award_financial"]["treasury_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + [
                "federal_owning_agency_name",
                "treasury_owning_agency_name",
                "last_modified_date",
                "reporting_fiscal_year",
                "reporting_fiscal_quarter",
                "reporting_fiscal_period",
                "quarter_format_flag",
                "submission_id",
                "federal_account_id",
                "funding_toptier_agency_id",
                "budget_function_code",
                "budget_subfunction_code",
            ]
        )
    )
    test_data_df = pd.DataFrame(
        data={
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "transaction_obligated_amount": [100, 100, 100, 100, 100],
            "submission_id": [1, 2, 3, 4, 5],
            "federal_owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "gross_outlay_amount_FYB_to_period_end": [100, 100, 100, 100, 100],
            "funding_toptier_agency_id": [1, 2, 2, 2, 3],
            "federal_account_id": [1, 2, 2, 2, 3],
        },
        columns=columns,
    ).fillna("dummy_text")
    (
        spark.createDataFrame(test_data_df)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("rpt.award_financial_download")
    )
    yield


@pytest.fixture(scope="function")
def account_balances_download_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        f"--destination-table=account_balances_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    test_data_df = pd.DataFrame(
        data={
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "submission_id": [1, 2, 3, 4, 5],
            "owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "gross_outlay_amount": [100, 100, 100, 100, 100],
        },
        columns=[field.name for field in account_balances_schema],
    ).fillna("dummy_text")
    (
        spark.createDataFrame(test_data_df)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("rpt.account_balances_download")
    )
    yield


@pytest.fixture
def agency_models(db):
    baker.make("references.ToptierAgency", pk=1, toptier_code="123")
    baker.make("references.ToptierAgency", pk=2, toptier_code="456")
    baker.make("references.ToptierAgency", pk=3, toptier_code="789")


@pytest.fixture
def federal_account_models(db):
    baker.make("accounts.FederalAccount", pk=1, agency_identifier="123", main_account_code="0111")
    baker.make("accounts.FederalAccount", pk=2, agency_identifier="234", main_account_code="0222")
    baker.make("accounts.FederalAccount", pk=3, agency_identifier="345", main_account_code="0333")


@patch("usaspending_api.download.delta_downloads.award_financial.get_submission_ids_for_periods")
def test_federal_award_financial_factory(
    mock_get_submission_ids_for_periods, spark, award_financial_table, agency_models
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_federal_account_download()
    result_df = result.dataframe.toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["A", "B; C; D"]
    assert sorted(result_df.transaction_obligated_amount.to_list()) == [100, 300]
    assert sorted(result_df.gross_outlay_amount_FYB_to_period_end.to_list()) == [100, 200]


@patch("usaspending_api.download.delta_downloads.award_financial.get_submission_ids_for_periods")
def test_filter_federal_by_agency(mock_get_submission_ids_for_periods, spark, award_financial_table, agency_models):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]

    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
        agency=2,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_federal_account_download()
    result_df = result.dataframe.toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["B; C; D"]
    assert result_df.transaction_obligated_amount.to_list() == [300]
    assert result_df.gross_outlay_amount_FYB_to_period_end.to_list() == [200]


@patch("usaspending_api.download.delta_downloads.award_financial.get_submission_ids_for_periods")
def test_filter_federal_by_federal_account_id(
    mock_get_submission_ids_for_periods, spark, award_financial_table, federal_account_models, agency_models
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]

    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
        federal_account=1,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_federal_account_download()
    result_df = result.dataframe.toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["A"]
    assert sorted(result_df.transaction_obligated_amount.to_list()) == [100]
    assert sorted(result_df.gross_outlay_amount_FYB_to_period_end.to_list()) == [100]


def test_treasury_award_financial_factory(spark, award_financial_table, agency_models):
    create_ref_temp_views(spark)
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_treasury_account_download()
    result_df = result.dataframe.toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["A", "B", "C", "D"]
        assert result_df.transaction_obligated_amount.to_list() == [100] * 4
        assert result_df.gross_outlay_amount_FYB_to_period_end.to_list() == [100] * 4


def test_filter_treasury_by_agency(spark, award_financial_table, agency_models):
    create_ref_temp_views(spark)
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
        agency=2,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_treasury_account_download()
    result_df = result.dataframe.toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["B", "C", "D"]
    assert result_df.transaction_obligated_amount.to_list() == [100] * 3
    assert result_df.gross_outlay_amount_FYB_to_period_end.to_list() == [100] * 3


@patch("usaspending_api.download.delta_downloads.account_balances.get_submission_ids_for_periods")
def test_account_balances(mock_get_submission_ids_for_periods, spark, account_balances_download_table, agency_models):
    mock_get_submission_ids_for_periods.return_value = [1, 2, 3]
    account_balances_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["account_balances"],
        quarter=4,
    )
    factory = AccountBalancesDownloadFactory(spark, account_balances_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframe.count() == 3
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframe.count() == 2
