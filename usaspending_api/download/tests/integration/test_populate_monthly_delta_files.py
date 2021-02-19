import zipfile
import datetime
import pytest
import os

from django.core.management import call_command
from os import listdir
from model_mommy import mommy
from csv import reader

from usaspending_api.settings import HOST
from usaspending_api.awards.models import TransactionDelta
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.download.tests.integration.test_populate_monthly_files import delete_files
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


@pytest.fixture
@pytest.mark.django_db(transaction=True)
def monthly_download_delta_data(db, monkeypatch):
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency")
    mommy.make("references.Agency", pk=1, toptier_agency_id=1)
    mommy.make("references.ToptierAgency", toptier_agency_id=2, toptier_code="002", name="Test_Agency 2")
    mommy.make("references.Agency", pk=2, toptier_agency_id=2)
    i = 1
    fiscal_year = 2020
    mommy.make(
        "awards.Award",
        id=i,
        generated_unique_award_id="CONT_AWD_1_0_0",
        is_fpds=True,
        type="B",
        type_description="Purchase Order",
        piid=f"piid{i}",
        awarding_agency_id=1,
        funding_agency_id=1,
        fiscal_year=fiscal_year,
    )
    mommy.make("awards.FinancialAccountsByAwards", award_id=i)
    mommy.make(
        "awards.TransactionNormalized",
        award_id=i,
        id=i,
        is_fpds=True,
        transaction_unique_id=i,
        usaspending_unique_transaction_id="",
        type="B",
        type_description="Purchase Order",
        period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
        period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
        action_date=datetime.datetime(fiscal_year, 5, 7),
        federal_action_obligation=100,
        modification_number="",
        description="a description",
        drv_award_transaction_usaspend=1,
        drv_current_total_award_value_amount_adjustment=1,
        drv_potential_total_award_value_amount_adjustment=1,
        last_modified_date=datetime.datetime(fiscal_year, 5, 7),
        certified_date=datetime.datetime(fiscal_year, 5, 7),
        create_date=datetime.datetime(fiscal_year, 5, 7),
        update_date=datetime.datetime(fiscal_year, 5, 7),
        fiscal_year=fiscal_year,
        awarding_agency_id=1,
        funding_agency_id=1,
        original_loan_subsidy_cost=100.0,
        face_value_loan_guarantee=100.0,
        funding_amount=100.0,
        non_federal_funding_amount=100.0,
        unique_award_key=1,
        business_categories=[],
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=i,
        detached_award_procurement_id=i,
        detached_award_proc_unique=f"test{i}",
        piid=f"piid{i}",
        agency_id=1,
        awarding_sub_tier_agency_c="001",
        awarding_sub_tier_agency_n="Test_Agency",
        awarding_agency_code="001",
        awarding_agency_name="Test_Agency",
        parent_award_id=f"000{i}",
        award_modification_amendme="1",
        contract_award_type="B",
        contract_award_type_desc="Contract",
        created_at=datetime.datetime(fiscal_year, 5, 7),
        updated_at=datetime.datetime(fiscal_year, 5, 7),
    )
    TransactionDelta.objects.update_or_create_transaction(i)

    monkeypatch.setenv("DOWNLOAD_DATABASE_URL", generate_test_db_connection_string())


@pytest.mark.django_db(transaction=True)
def test_all_agencies(monthly_download_delta_data, monkeypatch):
    call_command("populate_monthly_delta_files", "--debugging_skip_deleted", "--last_date=2020-12-31")
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_All_Contracts_Delta_{formatted_date}.zip" in file_list
    delete_files()


@pytest.mark.django_db(transaction=True)
def test_specific_agency(monthly_download_delta_data, monkeypatch):
    contract_data = [
        "C",
        "1",
        "test1",
        "CONT_AWD_1_0_0",
        "piid1",
        "{1}" "",
        "",
        "",
        "",
        "0001",
        "",
        "100.00",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "2020-05-07",
        "2020",
        "",
        "",
        "",
        "",
        "",
        "001",
        "Test_Agency",
        "001",
        "Test_Agency",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "B",
        "Contract",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        f"{HOST}/award/CONT_AWD_1_0_0/" if "localhost" in HOST else f"https://{HOST}/award/CONT_AWD_1_0_0/",
        "",
    ]
    call_command("populate_monthly_delta_files", "--agencies=1", "--debugging_skip_deleted", "--last_date=2020-12-31")
    file_list = listdir("csv_downloads")
    formatted_date = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")
    assert f"FY(All)_001_Contracts_Delta_{formatted_date}.zip" in file_list
    with zipfile.ZipFile(
        os.path.normpath(f"csv_downloads/FY(All)_001_Contracts_Delta_{formatted_date}.zip"), "r"
    ) as zip_ref:
        zip_ref.extractall("csv_downloads")
        assert f"FY(All)_001_Contracts_Delta_{formatted_date}_1.csv" in listdir("csv_downloads")
    with open(
        os.path.normpath(f"csv_downloads/FY(All)_001_Contracts_Delta_{formatted_date}_1.csv"), "r"
    ) as contract_file:
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


@pytest.mark.django_db(transaction=True)
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
        fiscal_year=2020,
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
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=2,
        fain="fain2",
        awarding_agency_code="001",
        awarding_sub_tier_agency_c=1,
        awarding_agency_name="Test_Agency",
        awarding_sub_tier_agency_n="Test_Agency",
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
