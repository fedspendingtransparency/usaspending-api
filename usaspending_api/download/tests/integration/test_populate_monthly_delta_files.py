import zipfile
import datetime
import pytest

from django.core.management import call_command
from os import listdir
from model_mommy import mommy
from csv import reader

from usaspending_api.awards.models import TransactionFPDS, TransactionDelta
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.download.tests.integration.test_populate_monthly_files import (
    delete_files,
    generate_contract_data,
)
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


@pytest.fixture
def monthly_download_delta_data(db, monkeypatch):
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency")
    mommy.make("references.Agency", pk=1, toptier_agency_id=1)
    mommy.make("references.ToptierAgency", toptier_agency_id=2, toptier_code="002", name="Test_Agency 2")
    mommy.make("references.Agency", pk=2, toptier_agency_id=2)
    fiscal_year = 2020
    mommy.make(
        "awards.Award",
        id=1,
        is_fpds=True,
        type="B",
        type_description="Purchase Order",
        piid=f"piid{1}",
        awarding_agency_id=1,
        funding_agency_id=1,
        latest_transaction_id=1,
        fiscal_year=fiscal_year,
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        detached_award_procurement_id=1,
        detached_award_proc_unique="test1",
        piid="piid1",
        agency_id=1,
        awarding_sub_tier_agency_c="001",
        awarding_sub_tier_agency_n="Test_Agency",
        awarding_agency_code="001",
        awarding_agency_name="Test_Agency",
        parent_award_id=f"000{1}",
        contract_award_type="B",
        contract_award_type_desc="Purchase Order",
        created_at=datetime.datetime(fiscal_year, 5, 7),
        updated_at=datetime.datetime(fiscal_year, 5, 7),
    )
    mommy.make(
        "awards.TransactionNormalized",
        award_id=1,
        id=1,
        is_fpds=True,
        transaction_unique_id=1,
        usaspending_unique_transaction_id="",
        type="B",
        type_description="Purchase Order",
        period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
        period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
        action_date=datetime.datetime(fiscal_year, 5, 7),
        last_modified_date=datetime.datetime(fiscal_year, 5, 7),
        certified_date=datetime.datetime(fiscal_year, 5, 7),
        create_date=datetime.datetime(fiscal_year, 5, 7),
        update_date=datetime.datetime(fiscal_year, 5, 7),
        fiscal_year=fiscal_year,
        awarding_agency_id=1,
        funding_agency_id=1,
    )
    mommy.make("awards.TransactionDelta", transaction_id=1, created_at=datetime.datetime.now())
    monkeypatch.setenv("DOWNLOAD_DATABASE_URL", generate_test_db_connection_string())


def test_all_agencies(client, monthly_download_delta_data, monkeypatch):
    transaction = TransactionFPDS.objects.get(transaction_id=1)
    transaction.updated_at = datetime.datetime.now()
    transaction.save()
    TransactionDelta.objects.update_or_create_transaction(1)
    call_command("populate_monthly_delta_files", "--debugging_skip_deleted", "--last_date=2020-12-31")
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_All_Contracts_Delta_{formatted_date}.zip" in file_list
    delete_files()


def test_specific_agency(client, monthly_download_delta_data, monkeypatch):
    call_command("populate_monthly_delta_files", "--agencies=1", "--debugging_skip_deleted", "--last_date=2020-12-31")
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_001_Contracts_Delta_{formatted_date}.zip" in file_list
    contract_data = generate_contract_data(2020, 1)
    contract_data.push("C")
    with zipfile.ZipFile(f"FY(All)_001_Contracts_Delta_{formatted_date}.zip", "r") as zip_ref:
        zip_ref.extractall("csv_downloads")
        assert f"FY2020_001_Contracts_Full_{formatted_date}_1.csv" in listdir("csv_downloads")
    with open(f"csv_downloads/FY2020_001_Contracts_Full_{formatted_date}_1.csv", "r") as contract_file:
        csv_reader = reader(contract_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                assert row == [s[:63] for s in query_paths["transaction"]["d1"].keys()]
            else:
                assert row == contract_data
            row_count += 1
    assert row_count >= 1
    delete_files()


def test_award_types(client, monthly_download_delta_data, monkeypatch):
    call_command(
        "populate_monthly_delta_files",
        "--agencies=1",
        "--award_types=assistance",
        "--debugging_skip_deleted",
        "--last_date=2020-12-31",
    )
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_001_Assistance_Delta_{formatted_date}.zip" not in file_list

    mommy.make(
        "awards.Award",
        id=2,
        is_fpds=False,
        type="02",
        type_description="Block Grant",
        fain="fain2",
        awarding_agency_id=2,
        funding_agency_id=2,
        latest_transaction_id=2,
        fiscal_year=2020,
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=2,
        fain="fain2",
        awarding_agency_code="001",
        awarding_sub_tier_agency_c=1,
        awarding_agency_name="Test_Agency",
        awarding_sub_tier_agency_n="Test_Agency",
    )
    mommy.make(
        "awards.TransactionNormalized",
        award_id=2,
        id=2,
        is_fpds=False,
        transaction_unique_id=2,
        type="02",
        type_description="Block Grant",
        period_of_performance_start_date=datetime.datetime(2020, 5, 7),
        period_of_performance_current_end_date=datetime.datetime(2020, 5, 7),
        action_date=datetime.datetime(2020, 5, 7),
        last_modified_date=datetime.datetime(2020, 5, 7),
        certified_date=datetime.datetime(2020, 5, 7),
        create_date=datetime.datetime(2020, 5, 7),
        update_date=datetime.datetime(2020, 5, 7),
        fiscal_year=2020,
        awarding_agency_id=1,
        funding_agency_id=1,
        unique_award_key=2,
    )
    mommy.make("awards.TransactionDelta", transaction_id=2, created_at=datetime.datetime.now())
    call_command(
        "populate_monthly_delta_files",
        "--agencies=1",
        "--award_types=assistance",
        "--debugging_skip_deleted",
        "--last_date=2020-12-31",
    )
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_001_Assistance_Delta_{formatted_date}.zip" in file_list
    delete_files()
