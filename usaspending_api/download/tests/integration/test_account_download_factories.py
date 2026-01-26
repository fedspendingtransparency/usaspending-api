from decimal import Decimal
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.download.delta_downloads.account_balances import AccountBalancesDownloadFactory
from usaspending_api.download.delta_downloads.award_financial import AwardFinancialDownloadFactory
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.delta_downloads.object_class_program_activity import (
    ObjectClassProgramActivityDownloadFactory,
)
from usaspending_api.download.delta_models.account_balances_download import account_balances_schema
from usaspending_api.download.delta_models.award_financial_download import award_financial_schema
from usaspending_api.download.delta_models.object_class_program_activity_download import (
    object_class_program_activity_schema,
)


@pytest.fixture(scope="function")
def award_financial_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        "--destination-table=award_financial_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    column_placeholders = {field.name: [None] * 5 for field in award_financial_schema}
    test_data_df = pd.DataFrame(
        data={
            **column_placeholders,
            "financial_accounts_by_awards_id": [1, 2, 3, 4, 5],
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "transaction_obligated_amount": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "submission_id": [1, 2, 3, 4, 5],
            "federal_owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_function_code": ["A100", "B100", "C100", "D100", "E100"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "budget_subfunction_code": ["A200", "B200", "C200", "D200", "E200"],
            "gross_outlay_amount_fyb_to_period_end": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "funding_toptier_agency_id": [1, 2, 2, 2, 3],
            "federal_account_id": [1, 2, 2, 2, 3],
            "disaster_emergency_fund_code": ["L", "L", "L", "L", "L"],
            "is_fpds": [True, True, True, True, True],
        }
    )
    # Some data manipulation for matching the Spark schema
    test_data_df[["reporting_fiscal_quarter", "reporting_fiscal_period"]] = test_data_df[
        ["reporting_fiscal_quarter", "reporting_fiscal_period"]
    ].astype(pd.Int8Dtype())
    test_data_df = test_data_df.replace([np.nan], [None])

    (
        spark.createDataFrame(test_data_df, schema=award_financial_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("rpt.award_financial_download")
    )
    yield


@pytest.fixture(scope="function")
def award_financial_table_award_category(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        "--destination-table=award_financial_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    column_placeholders = {field.name: [None] * 5 for field in award_financial_schema}
    test_data_df = pd.DataFrame(
        data={
            **column_placeholders,
            "financial_accounts_by_awards_id": [1, 2, 3, 4, 5],
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2018],
            "quarter_format_flag": [True, True, True, True, True],
            "reporting_fiscal_quarter": [1, 2, 3, 4, 2],
            "reporting_fiscal_period": [None, None, None, None, None],
            "transaction_obligated_amount": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "submission_id": [1, 2, 3, 4, 5],
            "federal_owning_agency_name": ["test1", "test2", "test3", "test4", "test5"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_function_code": ["A100", "B100", "C100", "D100", "E100"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "budget_subfunction_code": ["A200", "B200", "C200", "D200", "E200"],
            "gross_outlay_amount_fyb_to_period_end": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "funding_toptier_agency_id": [1, 1, 1, 1, 1],
            "federal_account_id": [1, 1, 1, 1, 1],
            "disaster_emergency_fund_code": ["L", "L", "L", "L", "L"],
            "is_fpds": [True, False, True, None, False],
        }
    )
    # Some data manipulation for matching the Spark schema
    test_data_df[["reporting_fiscal_quarter", "reporting_fiscal_period"]] = test_data_df[
        ["reporting_fiscal_quarter", "reporting_fiscal_period"]
    ].astype(pd.Int8Dtype())
    test_data_df = test_data_df.replace([np.nan], [None])

    (
        spark.createDataFrame(test_data_df, schema=award_financial_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("rpt.award_financial_download")
    )
    yield


@pytest.fixture(scope="function")
def account_balances_download_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        "--destination-table=account_balances_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    column_placeholders = {field.name: [None] * 5 for field in account_balances_schema}
    test_data_df = pd.DataFrame(
        data={
            **column_placeholders,
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "submission_id": [1, 2, 3, 4, 5],
            "owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function": ["A", "B", "C", "D", "E"],
            "budget_function_code": ["A100", "B100", "C100", "D100", "E100"],
            "budget_subfunction": ["A", "B", "C", "D", "E"],
            "budget_subfunction_code": ["A200", "B200", "C200", "D200", "E200"],
            "gross_outlay_amount": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "funding_toptier_agency_id": [1, 2, 2, 2, 3],
            "federal_account_id": [1, 2, 2, 2, 3],
        }
    )
    # Some data manipulation for matching the Spark schema
    test_data_df[["reporting_fiscal_quarter", "reporting_fiscal_period"]] = test_data_df[
        ["reporting_fiscal_quarter", "reporting_fiscal_period"]
    ].astype(pd.Int8Dtype())
    test_data_df = test_data_df.replace([np.nan], [None])

    (
        spark.createDataFrame(test_data_df, schema=account_balances_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("rpt.account_balances_download")
    )
    yield


@pytest.fixture(scope="function")
def object_class_by_program_activity_download_table(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    call_command(
        "create_delta_table",
        "--destination-table=object_class_program_activity_download",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    column_placeholders = {field.name: [None] * 5 for field in object_class_program_activity_schema}
    test_data_df = pd.DataFrame(
        data={
            **column_placeholders,
            "financial_accounts_by_program_activity_object_class_id": [1, 2, 3, 4, 5],
            "submission_id": [1, 2, 3, 4, 5],
            "owning_agency_name": ["test1", "test2", "test2", "test2", "test3"],
            "reporting_fiscal_year": [2018, 2018, 2018, 2018, 2019],
            "quarter_format_flag": [True, True, False, True, True],
            "reporting_fiscal_quarter": [1, 2, None, 4, 2],
            "reporting_fiscal_period": [None, None, 5, None, None],
            "reporting_agency_name": ["A", "B", "C", "D", "E"],
            "budget_function_title": [
                "BudgetFunction1",
                "BudgetFunction2",
                "BudgetFunction3",
                "BudgetFunction4",
                "BudgetFunction5",
            ],
            "budget_function_code": ["F01", "F02", "F03", "F04", "F05"],
            "budget_subfunction_title": [
                "BudgetSubFunction1",
                "BudgetSubFunction2",
                "BudgetSubFunction3",
                "BudgetSubFunction4",
                "BudgetSubFunction5",
            ],
            "budget_subfunction_code": ["SF01", "SF02", "SF03", "SF04", "SF05"],
            "gross_outlay_amount_FYB_to_period_end": [Decimal(100).quantize(Decimal(".01"))] * 5,
            "funding_toptier_agency_id": [1, 2, 2, 2, 3],
            "federal_account_id": [1, 2, 2, 2, 3],
            "disaster_emergency_fund_code": ["L", "L", "L", "L", "L"],
        },
    )
    # Some data manipulation for matching the Spark schema
    test_data_df[["reporting_fiscal_quarter", "reporting_fiscal_period"]] = test_data_df[
        ["reporting_fiscal_quarter", "reporting_fiscal_period"]
    ].astype(pd.Int8Dtype())
    test_data_df = test_data_df.replace([np.nan], [None])

    (
        spark.createDataFrame(test_data_df, schema=object_class_program_activity_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable("rpt.object_class_program_activity_download")
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


@patch("usaspending_api.common.spark.utils.get_submission_ids_for_periods")
def test_federal_award_financial_factory(
    mock_get_submission_ids_for_periods, spark, award_financial_table, agency_models, federal_account_models
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 4, 5]

    # Test filters return multiple records
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    result = factory.create_federal_account_download()
    result_df = result.dataframes[1].toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["A", "B; C; D"]
    assert sorted(result_df.transaction_obligated_amount.to_list()) == [100, 300]
    assert sorted(result_df.gross_outlay_amount_FYB_to_period_end.to_list()) == [100, 200]

    # Test all the filters
    award_financial_filter = AccountDownloadFilters(
        fy="2018",
        submission_types=["award_financial"],
        quarter="4",
        period="11",
        agency="1",
        federal_account="1",
        budget_function="A100",
        budget_subfunction="A200",
        def_codes=["L"],
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframes[1].count() == 1
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframes[1].count() == 1


@patch("usaspending_api.common.spark.utils.get_submission_ids_for_periods")
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
    result_df = result.dataframes[1].toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["B; C; D"]
    assert result_df.transaction_obligated_amount.to_list() == [300]
    assert result_df.gross_outlay_amount_FYB_to_period_end.to_list() == [200]


@patch("usaspending_api.common.spark.utils.get_submission_ids_for_periods")
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
    result_df = result.dataframes[1].toPandas()
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
    result_df = result.dataframes[1].toPandas()
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
    result_df = result.dataframes[1].toPandas()
    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]:
        assert sorted(result_df[col].to_list()) == ["B", "C", "D"]
    assert result_df.transaction_obligated_amount.to_list() == [100] * 3
    assert result_df.gross_outlay_amount_FYB_to_period_end.to_list() == [100] * 3


def test_file_c_filenames(spark, award_financial_table, agency_models):
    create_ref_temp_views(spark)
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
        agency=2,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    ta_result = factory.create_treasury_account_download()
    assert "Assistance" in ta_result.file_names[0]
    assert "Contracts" in ta_result.file_names[1]
    assert "Unlinked" in ta_result.file_names[2]

    fa_result = factory.create_federal_account_download()
    assert "Assistance" in fa_result.file_names[0]
    assert "Contracts" in fa_result.file_names[1]
    assert "Unlinked" in fa_result.file_names[2]


@patch("usaspending_api.common.spark.utils.get_submission_ids_for_periods")
def test_file_c_award_categories(
    mock_get_submission_ids_for_periods, spark, award_financial_table_award_category, agency_models
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 3, 4, 5]
    award_financial_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["award_financial"],
        quarter=4,
    )
    factory = AwardFinancialDownloadFactory(spark, award_financial_filter)
    ta_result = factory.create_treasury_account_download()
    assert sorted(ta_result.dataframes[0].toPandas()["reporting_agency_name"].to_list()) == ["B", "E"]
    assert sorted(ta_result.dataframes[1].toPandas()["reporting_agency_name"].to_list()) == ["A", "C"]
    assert sorted(ta_result.dataframes[2].toPandas()["reporting_agency_name"].to_list()) == ["D"]

    fa_result = factory.create_federal_account_download()
    assert sorted(fa_result.dataframes[0].toPandas()["reporting_agency_name"].to_list()) == ["B", "E"]
    assert sorted(fa_result.dataframes[1].toPandas()["reporting_agency_name"].to_list()) == ["A", "C"]
    assert sorted(fa_result.dataframes[2].toPandas()["reporting_agency_name"].to_list()) == ["D"]


@patch("usaspending_api.download.delta_downloads.account_balances.get_submission_ids_for_periods")
def test_account_balances(
    mock_get_submission_ids_for_periods, spark, account_balances_download_table, agency_models, federal_account_models
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 3]

    # Test filters return multiple records
    account_balances_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["account_balances"],
        quarter=4,
    )
    factory = AccountBalancesDownloadFactory(spark, account_balances_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframes[0].count() == 3
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframes[0].count() == 2

    # Test all the filters
    account_balances_filter = AccountDownloadFilters(
        fy="2018",
        submission_types=["account_balances"],
        quarter="4",
        period="11",
        agency="1",
        federal_account="1",
        budget_function="A100",
        budget_subfunction="A200",
        def_codes=["L"],
    )
    factory = AccountBalancesDownloadFactory(spark, account_balances_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframes[0].count() == 1
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframes[0].count() == 1


@patch("usaspending_api.download.delta_downloads.object_class_program_activity.get_submission_ids_for_periods")
def test_object_class_by_program_activity(
    mock_get_submission_ids_for_periods,
    spark,
    object_class_by_program_activity_download_table,
    agency_models,
    federal_account_models,
):
    create_ref_temp_views(spark)
    mock_get_submission_ids_for_periods.return_value = [1, 2, 3]

    # Test filters return multiple records
    ocpa_filter = AccountDownloadFilters(
        fy=2018,
        submission_types=["object_class_program_activity"],
        quarter=4,
    )
    factory = ObjectClassProgramActivityDownloadFactory(spark, ocpa_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframes[0].count() == 3
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframes[0].count() == 2

    # Test all the filters
    ocpa_filter = AccountDownloadFilters(
        fy="2018",
        submission_types=["object_class_program_activity"],
        quarter="4",
        period="11",
        agency="1",
        federal_account="1",
        budget_function="F01",
        budget_subfunction="SF01",
        def_codes=["L"],
    )
    factory = ObjectClassProgramActivityDownloadFactory(spark, ocpa_filter)
    ta_dataframe = factory.create_treasury_account_download()
    assert ta_dataframe.dataframes[0].count() == 1
    fa_dataframe = factory.create_federal_account_download()
    assert fa_dataframe.dataframes[0].count() == 1
