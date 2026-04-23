import logging
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api import settings
from usaspending_api.common.helpers.s3_helpers import retrieve_s3_bucket_object_list
from usaspending_api.download.delta_models.transaction_download import transaction_download_schema


@pytest.fixture
def agency_models(db):
    dod_agency = baker.make("references.ToptierAgency", pk=11, toptier_code="097", abbreviation="DOD")
    dod_sub_agency = baker.make("references.SubtierAgency", pk=12, subtier_code="0091", abbreviation="DOD_PLACEHOLDER")
    usda_agency = baker.make("references.ToptierAgency", pk=21, toptier_code="012", abbreviation="USDA")
    usda_sub_agency = baker.make(
        "references.SubtierAgency", pk=22, subtier_code="0012", abbreviation="USDA_PLACEHOLDER"
    )
    baker.make(
        "references.Agency", pk=10, toptier_agency=dod_agency, subtier_agency=dod_sub_agency, user_selectable=True
    )
    baker.make(
        "references.Agency", pk=20, toptier_agency=usda_agency, subtier_agency=usda_sub_agency, user_selectable=True
    )


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
    yield s3_unittest_data_bucket


@patch("usaspending_api.download.management.commands.generate_monthly_download.datetime")
def test_generate_full_assistance_and_contract(
    mock_datetime, spark, transaction_download_table, agency_models, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(
        "usaspending_api.download.management.commands.generate_monthly_download.logger", logging.getLogger()
    )

    mock_datetime.strptime.return_value = datetime.strptime(settings.API_SEARCH_MIN_DATE, "%Y-%m-%d")
    mock_datetime.strftime.return_value = "20200130"

    agency_args = ["-a", "DOD", "USDA"]
    fy_args = ["-fy", "2020"]
    monthly_type_arg = ["-m", "full"]
    bucket_arg = ["-b", f"{transaction_download_table}"]

    # Validate that initial run will generate files according to the filters
    call_command("generate_monthly_download", *agency_args, *fy_args, *monthly_type_arg, *bucket_arg)
    sorted_keys = sorted(
        [obj.key for obj in retrieve_s3_bucket_object_list(transaction_download_table, key_prefix="FY")]
    )
    expected_results = [
        ("FY2020_012_Assistance_Full_20200130.zip", 0),
        ("FY2020_012_Contracts_Full_20200130.zip", 1),
        ("FY2020_097_Assistance_Full_20200130.zip", 1),
        ("FY2020_097_Contracts_Full_20200130.zip", 0),
    ]
    assert sorted_keys == [file_name for file_name, _ in expected_results]
    for file_name, row_count in expected_results:
        file_prefix = file_name.replace(".zip", "")
        expected_message = (
            f"s3a://{transaction_download_table}/temp_download/{file_prefix} contains {row_count} rows of data"
        )
        assert expected_message in caplog.messages

    # Validate that a second run without "--overwrite" skips the files that were previously created
    caplog.clear()
    call_command("generate_monthly_download", *agency_args, *fy_args, *monthly_type_arg, *bucket_arg)
    for file_name, _ in expected_results:
        file_prefix = file_name.replace(".zip", "")
        assert f"Skipping {file_prefix}" in caplog.messages

    # Validate that using the "--overwrite" command processes the files even though they exist
    caplog.clear()
    call_command(
        "generate_monthly_download",
        *fy_args,
        *monthly_type_arg,
        *bucket_arg,
        "--award-categories",
        "assistance",
        "--all-agencies",
        "--overwrite",
    )
    for file_name, row_count in expected_results:
        file_prefix = file_name.replace(".zip", "")
        expected_message = (
            f"s3a://{transaction_download_table}/temp_download/{file_prefix} contains {row_count} rows of data"
        )
        if "Assistance" in file_prefix:
            assert expected_message in caplog.messages
        else:
            assert expected_message not in caplog.messages
