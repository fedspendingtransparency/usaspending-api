import datetime
import os
import zipfile
from csv import reader
from unittest.mock import Mock

import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api import settings
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.config import CONFIG
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths

# Make sure UTC or test will fail later in the day
TODAY = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y%m%d")


def generate_contract_data(fiscal_year, i):
    return [
        i,
        i,
        f"piid{i}",
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
        f"05/07{fiscal_year}",
        f"05/07/{fiscal_year}",
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
        "",
        "",
        "",
        "",
        "",
        f"www.usaspending.gov/award/CONT_AWD_{i}_0_0",
        f"05/07/{fiscal_year}",
    ]


def generate_assistance_data(fiscal_year, i):
    return [
        i + 100,
        i + 100,
        f"fain{i + 100}",
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
        "",
        "",
        "",
        "02",
        "Block Grant",
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
        f"http://www.usaspending.gov/award/ASST_NON_{i}_0_0",
        f"05/07/{fiscal_year}",
    ]


@pytest.fixture
def monthly_download_data(db, monkeypatch):
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    baker.make(
        "references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency", _fill_optional=True
    )
    baker.make("references.Agency", pk=1, toptier_agency_id=1, _fill_optional=True)
    baker.make(
        "references.ToptierAgency", toptier_agency_id=2, toptier_code="002", name="Test_Agency 2", _fill_optional=True
    )
    baker.make("references.Agency", pk=2, toptier_agency_id=2, _fill_optional=True)
    i = 1
    for fiscal_year in range(2001, 2021):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            generated_unique_award_id=f"CONT_AWD_{i}_0_0",
            is_fpds=True,
            type="B",
            type_description="Purchase Order",
            piid=f"piid{i}",
            awarding_agency_id=1,
            funding_agency_id=1,
            latest_transaction_id=i,
            fiscal_year=fiscal_year,
        )
        baker.make(
            "search.TransactionSearch",
            award_id=i,
            transaction_id=i,
            is_fpds=True,
            transaction_unique_id=i,
            usaspending_unique_transaction_id="",
            type="B",
            type_description="Purchase Order",
            period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
            period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
            action_date=datetime.datetime(fiscal_year, 5, 7),
            federal_action_obligation=100,
            modification_number="1",
            transaction_description="a description",
            last_modified_date=datetime.datetime(fiscal_year, 5, 7),
            award_certified_date=datetime.datetime(fiscal_year, 5, 7),
            create_date=datetime.datetime(fiscal_year, 5, 7),
            update_date=datetime.datetime(fiscal_year, 5, 7),
            fiscal_year=fiscal_year,
            awarding_agency_id=1,
            funding_agency_id=1,
            original_loan_subsidy_cost=100.0,
            face_value_loan_guarantee=100.0,
            funding_amount=100.0,
            non_federal_funding_amount=100.0,
            generated_unique_award_id=f"CONT_AWD_{i}_0_0",
            business_categories=[],
            detached_award_proc_unique=f"test{i}",
            piid=f"piid{i}",
            agency_id=1,
            awarding_sub_tier_agency_c="001",
            awarding_subtier_agency_abbreviation="Test_Agency",
            awarding_agency_code="001",
            awarding_toptier_agency_abbreviation="Test_Agency",
            parent_award_id=f"000{i}",
        )
        baker.make(
            "search.AwardSearch",
            award_id=i + 100,
            generated_unique_award_id=f"ASST_NON_{i}_0_0",
            is_fpds=False,
            type="02",
            type_description="Block Grant",
            fain=f"fain{i}",
            awarding_agency_id=1,
            funding_agency_id=1,
            latest_transaction_id=i + 100,
            fiscal_year=fiscal_year,
        )
        baker.make(
            "search.TransactionSearch",
            award_id=i + 100,
            generated_unique_award_id=f"ASST_NON_{i}_0_0",
            transaction_id=i + 100,
            is_fpds=False,
            transaction_unique_id=i + 100,
            usaspending_unique_transaction_id="",
            type="02",
            type_description="Block Grant",
            period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
            period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
            action_date=datetime.datetime(fiscal_year, 5, 7),
            federal_action_obligation=100,
            modification_number=f"{i + 100}",
            transaction_description="a description",
            last_modified_date=datetime.datetime(fiscal_year, 5, 7),
            award_certified_date=datetime.datetime(fiscal_year, 5, 7),
            create_date=datetime.datetime(fiscal_year, 5, 7),
            update_date=datetime.datetime(fiscal_year, 5, 7),
            fiscal_year=fiscal_year,
            awarding_agency_id=1,
            funding_agency_id=1,
            original_loan_subsidy_cost=100.0,
            face_value_loan_guarantee=100.0,
            funding_amount=100.0,
            non_federal_funding_amount=100.0,
            fain=f"fain{i}",
            awarding_agency_code="001",
            awarding_sub_tier_agency_c=1,
            awarding_toptier_agency_abbreviation="Test_Agency",
            awarding_subtier_agency_abbreviation="Test_Agency",
        )
        i += 1
    monkeypatch.setattr(CONFIG, "MONTHLY_DOWNLOAD_S3_BUCKET_NAME", "whatever")


@pytest.mark.django_db(databases=[settings.BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_all_agencies(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command("populate_monthly_files", "--fiscal_year=2020", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)

    assert f"FY2020_All_Contracts_Full_{TODAY}.zip" in file_list
    assert f"FY2020_All_Assistance_Full_{TODAY}.zip" in file_list


@pytest.mark.django_db(databases=[settings.BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_specific_agency(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    contract_data = generate_contract_data(2020, 1)
    assistance_data = generate_assistance_data(2020, 1)
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)

    assistance_csv_1 = f"FY2020_001_Assistance_Full_{TODAY}_1.csv"
    assistance_zip_1 = f"FY2020_001_Assistance_Full_{TODAY}.zip"
    contracts_csv_1 = f"FY2020_001_Contracts_Full_{TODAY}_1.csv"
    contracts_zip_1 = f"FY2020_001_Contracts_Full_{TODAY}.zip"

    assert contracts_zip_1 in file_list
    assert assistance_zip_1 in file_list

    with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{contracts_zip_1}"), "r") as zip_ref:
        zip_ref.extractall(fake_csv_local_path)
        assert contracts_csv_1 in os.listdir(fake_csv_local_path)
    with open(os.path.normpath(f"{fake_csv_local_path}/{contracts_csv_1}"), "r") as contract_file:
        csv_reader = reader(contract_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                # 63 is the character limit for column names
                assert row == [s[:63] for s in query_paths["transaction_search"]["d1"].keys()]
            else:
                assert row == contract_data
            row_count += 1
    assert row_count >= 1

    with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{assistance_zip_1}"), "r") as zip_ref:
        zip_ref.extractall(fake_csv_local_path)
        assert assistance_csv_1 in os.listdir(fake_csv_local_path)
    with open(os.path.normpath(f"{fake_csv_local_path}/{assistance_csv_1}"), "r") as assistance_file:
        csv_reader = reader(assistance_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                # 63 is the character limit for column names
                assert row == [s[:63] for s in query_paths["transaction_search"]["d2"].keys()]
            else:
                assert row == assistance_data
            row_count += 1
        assert row_count >= 1


@pytest.mark.django_db(databases=[settings.BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_agency_no_data(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command("populate_monthly_files", "--agencies=2", "--fiscal_year=2022", "--local", "--clobber")
    contracts_zip = f"FY2022_002_Contracts_Full_{TODAY}.zip"
    contracts_csv_1 = f"FY2022_002_Contracts_Full_{TODAY}_1.csv"
    assistance_zip = f"FY2022_002_Assistance_Full_{TODAY}.zip"
    assistance_csv_1 = f"FY2022_002_Assistance_Full_{TODAY}_1.csv"

    for zip_file, csv_file in [(contracts_zip, contracts_csv_1), (assistance_zip, assistance_csv_1)]:
        with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{zip_file}"), "r") as zip_ref:
            zip_ref.extractall(fake_csv_local_path)
            assert csv_file in os.listdir(fake_csv_local_path), f"{csv_file} was not generated or extracted"

        with open(os.path.normpath(f"{fake_csv_local_path}/{csv_file}"), "r") as csv_file:
            csv_reader = reader(csv_file)
            row_count = 0
            for _ in csv_reader:
                row_count += 1
            assert row_count == 1, f"{csv_file} was not empty"


@pytest.mark.django_db(databases=[settings.BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_fiscal_years(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    # contract_data = generate_contract_data(2020, 1)
    # assistance_data = generate_assistance_data(2020, 1)
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2004", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)
    expected_files = (
        f"FY2004_001_Contracts_Full_{TODAY}.zip",
        f"FY2004_001_Assistance_Full_{TODAY}.zip",
        f"FY2020_001_Contracts_Full_{TODAY}.zip",
        f"FY2020_001_Assistance_Full_{TODAY}.zip",
    )

    for expected_file in expected_files:
        assert expected_file in file_list


@pytest.mark.django_db(databases=[settings.BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_award_type(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command(
        "populate_monthly_files",
        "--agencies=1",
        "--fiscal_year=2020",
        "--award_types=assistance",
        "--local",
        "--clobber",
    )
    file_list = os.listdir(fake_csv_local_path)

    assert f"FY2020_001_Assistance_Full_{TODAY}.zip" in file_list
    assert f"FY2020_001_Contracts_Full_{TODAY}.zip" not in file_list
