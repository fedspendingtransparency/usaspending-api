import pytest
import datetime
import zipfile

from django.core.management import call_command
from os import listdir
from csv import reader
from model_mommy import mommy
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


@pytest.fixture
def monthly_download_data(db, monkeypatch):
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency")
    mommy.make("references.Agency", pk=1, toptier_agency_id=1)
    mommy.make(
        "awards.Award",
        id=1,
        is_fpds=True,
        type="B",
        type_description="Purchase Order",
        piid="piid1",
        awarding_agency_id=1,
        funding_agency_id=1,
        latest_transaction_id=1,
        fiscal_year=2020,
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
        parent_award_id="0001",
        award_modification_amendme="1",
        type_of_contract_pricing="",
        type_of_contract_pric_desc="",
        contract_award_type="B",
        contract_award_type_desc="Contract",
        created_at=datetime.datetime(2020, 5, 7),
        updated_at=datetime.datetime(2020, 5, 7),
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
        period_of_performance_start_date=datetime.datetime(2020, 5, 7),
        period_of_performance_current_end_date=datetime.datetime(2020, 5, 7),
        action_date=datetime.datetime(2020, 5, 7),
        action_type="",
        action_type_description="",
        federal_action_obligation=100,
        modification_number="",
        description="",
        drv_award_transaction_usaspend=1,
        drv_current_total_award_value_amount_adjustment=1,
        drv_potential_total_award_value_amount_adjustment=1,
        last_modified_date=datetime.datetime(2020, 5, 7),
        certified_date=datetime.datetime(2020, 5, 7),
        create_date=datetime.datetime(2020, 5, 7),
        update_date=datetime.datetime(2020, 5, 7),
        fiscal_year=2020,
        awarding_agency_id=1,
        funding_agency_id=1,
        original_loan_subsidy_cost=100.0,
        face_value_loan_guarantee=100.0,
        funding_amount=100.0,
        non_federal_funding_amount=100.0,
        unique_award_key=1,
        business_categories=[],
    )
    monkeypatch.setattr("usaspending_api.settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME", "whatever")


def test_all_agencies(client, monthly_download_data, monkeypatch):
    call_command("populate_monthly_files", "--fiscal_year=2020", "--local", "--clobber")
    file_list = listdir("csv_downloads")
    assert len(file_list) > 3


def test_specific_agency(client, monthly_download_data):
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY2020_001_Contracts_Full_{formatted_date}.zip" in file_list
    assert f"FY2020_001_Assistance_Full_{formatted_date}.zip" in file_list

    with zipfile.ZipFile(f"csv_downloads/FY2020_001_Contracts_Full_{formatted_date}.zip", "r") as zip_ref:
        zip_ref.extractall("csv_downloads")
        assert f"FY2020_001_Contracts_Full_{formatted_date}_1.csv" in listdir("csv_downloads")
    with open(f"csv_downloads/FY2020_001_Contracts_Full_{formatted_date}_1.csv", "r") as contract_file:
        csv_reader = reader(contract_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                assert row == [s[:63] for s in query_paths["transaction"]["d1"].keys()]
            row_count += 1

    with zipfile.ZipFile(f"csv_downloads/FY2020_001_Assistance_Full_{formatted_date}.zip", "r") as zip_ref:
        zip_ref.extractall("csv_downloads")
        assert f"FY2020_001_Assistance_Full_{formatted_date}_1.csv" in listdir("csv_downloads")
    with open(f"csv_downloads/FY2020_001_Assistance_Full_{formatted_date}_1.csv", "r") as assistance_file:
        csv_reader = reader(assistance_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                assert row == [s[:63] for s in query_paths["transaction"]["d2"].keys()]
            row_count += 1


def test_award_types(client, monthly_download_data):
    assert False


def test_fiscal_years(client, monthly_download_data):
    assert False


def test_fail_state(client, monthly_download_data):
    assert False
