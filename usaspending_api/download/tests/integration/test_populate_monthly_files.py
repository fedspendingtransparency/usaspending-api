import pytest
import datetime
import zipfile

from django.core.management import call_command
from os import listdir
from csv import reader
from model_mommy import mommy

@pytest.fixture
def monthly_download_data(db, monkeypatch):
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency")
    mommy.make("references.Agency", pk=1, toptier_agency_id=1)
    mommy.make("awards.TransactionNormalized")
    monkeypatch.setattr("usaspending_api.settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME", "whatever")

def test_all_agencies(client, monthly_download_data, monkeypatch):
    call_command("populate_monthly_files","--fiscal_year=2020","--local", "--clobber")
    file_list = listdir("csv_downloads")
    assert len(file_list) > 3

def test_specific_agency(client, monthly_download_data):
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    file_list = listdir("csv_downloads")
    formated_date = datetime.datetime.strftime(datetime.date.today(), '%Y%m%d')
    assert f"FY2020_001_Contracts_Full_{formated_date}.zip" in file_list
    assert f"FY2020_001_Assistance_Full_{formated_date}.zip" in file_list

    with zipfile.ZipFile(f"csv_downloads/FY2020_001_Contracts_Full_{datetime.datetime.strftime(datetime.date.today(), '%Y%m%d')}.zip", 'r') as zip_ref:
        zip_ref.extractall("csv_downloads")
        assert f"FY2020_001_Contracts_Full_{formated_date}_1.csv" in listdir("csv_downloads")
    with open(f"csv_downloads/FY2020_001_Contracts_Full_{formated_date}_1.csv", "r") as contract_file:
        csv_reader = reader(contract_file)
        for row in csv_reader:
            print(row)

    assert False



def test_award_types(client, monthly_download_data):
    assert False

def test_fiscal_years(client, monthly_download_data):
    assert False

def test_fail_state(client, monthly_download_data):
    assert False