from unittest.mock import patch

import pandas as pd
import pytest
from django.core.management import call_command
from usaspending_api.download.management.commands.delta_downloads.award_financial.columns import (
    federal_account_select_cols,
    federal_account_groupby_cols,
)
from usaspending_api.download.management.commands.delta_downloads.award_financial.federal_account import (
    AccountDownloadDataFrameBuilder,
    AccountDownloadFilter,
)


@pytest.fixture(scope="function")
def account_download_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        f"--destination-table=account_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    columns = list(set(federal_account_select_cols + federal_account_groupby_cols)) + [
        "reporting_fiscal_year",
        "reporting_fiscal_quarter",
        "reporting_fiscal_period",
        "quarter_format_flag",
        "submission_id",
        "agency_code",
        "federal_account_id",
    ]
    test_data_df = pd.DataFrame(
        data={
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "transaction_obligated_amount": [100, 100, 100, 100, 100],
            "submission_id": [1, 2, 3, 4, 5],
            "owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "gross_outlay_amount_FYB_to_period_end": [100, 100, 100, 100, 100],
            "agency_code": [1, 2, 2, 2, 3],
            "federal_account_id": [1, 2, 2, 2, 3],
        },
        columns=columns,
    ).fillna("dummy_text")
    (
        spark.createDataFrame(test_data_df)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("rpt.account_download")
    )
    yield


@patch(
    "usaspending_api.download.management.commands.delta_downloads.award_financial.federal_account.get_submission_ids_for_periods"
)
def test_account_download_dataframe_builder(mock_get_submission_ids_for_periods, spark, account_download_table):
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]
    account_download_filter = AccountDownloadFilter(
        year=2018,
        quarter=4,
    )
    builder = AccountDownloadDataFrameBuilder(spark, account_download_filter, "rpt.account_download")
    result = builder.source_df
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result.toPandas()[col].to_list()) == ["A", "B; C; D"]
    assert sorted(result.toPandas().transaction_obligated_amount.to_list()) == [100, 300]
    assert sorted(result.toPandas().gross_outlay_amount_FYB_to_period_end.to_list()) == [100, 200]


@patch(
    "usaspending_api.download.management.commands.delta_downloads.award_financial.federal_account.get_submission_ids_for_periods"
)
def test_filter_by_agency(mock_get_submission_ids_for_periods, spark, account_download_table):
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]

    account_download_filter = AccountDownloadFilter(
        year=2018,
        quarter=4,
        agency=2,
    )
    builder = AccountDownloadDataFrameBuilder(spark, account_download_filter)
    result = builder.source_df
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result.toPandas()[col].to_list()) == ["B; C; D"]
    assert sorted(result.toPandas().transaction_obligated_amount.to_list()) == [300]
    assert sorted(result.toPandas().gross_outlay_amount_FYB_to_period_end.to_list()) == [200]


@patch(
    "usaspending_api.download.management.commands.delta_downloads.award_financial.federal_account.get_submission_ids_for_periods"
)
def test_filter_by_federal_account_id(mock_get_submission_ids_for_periods, spark, account_download_table):
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]

    account_download_filter = AccountDownloadFilter(
        year=2018,
        quarter=4,
        federal_account_id=1,
    )
    builder = AccountDownloadDataFrameBuilder(spark, account_download_filter)
    result = builder.source_df
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result.toPandas()[col].to_list()) == ["A"]
    assert sorted(result.toPandas().transaction_obligated_amount.to_list()) == [100]
    assert sorted(result.toPandas().gross_outlay_amount_FYB_to_period_end.to_list()) == [100]
