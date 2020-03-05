import json
import pytest


from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.awards.models import (
    TransactionNormalized,
    TransactionFABS,
    TransactionFPDS,
    Subaward,
    BrokerSubaward,
)
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards


@pytest.fixture
def award_data(transactional_db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Awarding Top Agency
    ata1 = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Things",
        toptier_code="100",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )
    ata2 = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Awarding subs
    asa1 = mommy.make("references.SubtierAgency", name="Bureau of Things")
    asa2 = mommy.make("references.SubtierAgency", name="Bureau of Stuff")

    # Create Awarding Agencies
    aa1 = mommy.make("references.Agency", toptier_agency=ata1, subtier_agency=asa1, toptier_flag=False)
    aa2 = mommy.make("references.Agency", toptier_agency=ata2, subtier_agency=asa2, toptier_flag=False)

    # Create Funding Top Agency
    fta = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding SUB
    fsa1 = mommy.make("references.SubtierAgency", name="Bureau of Things")

    # Create Funding Agency
    mommy.make("references.Agency", toptier_agency=fta, subtier_agency=fsa1, toptier_flag=False)

    # Create Federal Account
    mommy.make("accounts.FederalAccount", account_title="Compensation to Accounts", agency_identifier="102", id=1)

    # Create Awards
    mommy.make("awards.Award", id=1, category="contracts", generated_unique_award_id="TEST_AWARD_1")
    mommy.make("awards.Award", id=2, category="contracts", generated_unique_award_id="TEST_AWARD_2")
    mommy.make("awards.Award", id=3, category="assistance", generated_unique_award_id="TEST_AWARD_3")
    mommy.make("awards.Award", id=4, category="contracts", generated_unique_award_id="TEST_AWARD_4")
    mommy.make("awards.Award", id=5, category="assistance", generated_unique_award_id="TEST_AWARD_5")
    mommy.make("awards.Award", id=6, category="assistance", generated_unique_award_id="TEST_AWARD_6")

    # Create Transactions
    mommy.make(
        TransactionNormalized,
        id=1,
        award_id=1,
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="TEST_AWARD_1",
        action_date="2017-01-01",
        type="A",
    )
    mommy.make(
        TransactionNormalized,
        id=2,
        award_id=2,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_2",
        action_date="2017-04-01",
        type="IDV_B",
    )
    mommy.make(
        TransactionNormalized,
        id=3,
        award_id=3,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_3",
        action_date="2017-06-01",
        type="02",
    )
    mommy.make(
        TransactionNormalized,
        id=4,
        award_id=4,
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="TEST_AWARD_4",
        action_date="2018-01-15",
        type="A",
    )
    mommy.make(
        TransactionNormalized,
        id=5,
        award_id=5,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_5",
        action_date="2018-03-15",
        type="07",
    )
    mommy.make(
        TransactionNormalized,
        id=6,
        award_id=6,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_6",
        action_date="2018-06-15",
        type="02",
    )

    # Create Transactions
    mommy.make(
        TransactionNormalized,
        id=1,
        award_id=1,
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="TEST_AWARD_1",
        action_date="2017-01-01",
        type="A",
    )
    mommy.make(
        TransactionNormalized,
        id=2,
        award_id=2,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_2",
        action_date="2017-04-01",
        type="IDV_B",
    )
    mommy.make(
        TransactionNormalized,
        id=3,
        award_id=3,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_3",
        action_date="2017-06-01",
        type="02",
    )
    mommy.make(
        TransactionNormalized,
        id=4,
        award_id=4,
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="TEST_AWARD_4",
        action_date="2018-01-15",
        type="A",
    )
    mommy.make(
        TransactionNormalized,
        id=5,
        award_id=5,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_5",
        action_date="2018-03-15",
        type="07",
    )
    mommy.make(
        TransactionNormalized,
        id=6,
        award_id=6,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_6",
        action_date="2018-06-15",
        type="02",
    )

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction_id=1, piid="tc1piid", unique_award_key="TEST_AWARD_1")
    mommy.make(TransactionFPDS, transaction_id=2, piid="tc2piid", unique_award_key="TEST_AWARD_2")
    mommy.make(TransactionFPDS, transaction_id=4, piid="tc4piid", unique_award_key="TEST_AWARD_4")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction_id=3, fain="ta1fain", unique_award_key="TEST_AWARD_3")
    mommy.make(TransactionFABS, transaction_id=5, fain="ta5fain", unique_award_key="TEST_AWARD_5")
    mommy.make(TransactionFABS, transaction_id=6, fain="ta6fain", unique_award_key="TEST_AWARD_6")

    # Create Subaward
    mommy.make(Subaward, id=1, award_id=4, latest_transaction_id=4, action_date="2018-01-15", award_type="procurement")
    mommy.make(Subaward, id=2, award_id=5, latest_transaction_id=5, action_date="2018-03-15", award_type="grant")
    mommy.make(Subaward, id=3, award_id=6, latest_transaction_id=6, action_date="2018-06-15", award_type="grant")

    # Create BrokerSubaward
    mommy.make(BrokerSubaward, id=1, prime_id=4, action_date="2018-01-15", subaward_type="sub-contract")
    mommy.make(BrokerSubaward, id=2, prime_id=5, action_date="2018-03-15", subaward_type="sub-grant")
    mommy.make(BrokerSubaward, id=3, prime_id=6, action_date="2018-06-15", subaward_type="sub-grant")

    # Set latest_award for each award
    update_awards()


@pytest.mark.skip
def test_download_transactions_v2_endpoint(client, award_data):
    """test the transaction endpoint."""

    resp = client.post(
        "/api/v2/bulk_download/transactions",
        content_type="application/json",
        data=json.dumps({"filters": {}, "columns": {}}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.skip
def test_download_awards_v2_endpoint(client, award_data):
    """test the awards endpoint."""

    resp = client.post(
        "/api/v2/bulk_download/awards", content_type="application/json", data=json.dumps({"filters": {}, "columns": []})
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.skip
def test_download_transactions_v2_status_endpoint(client, award_data):
    """Test the transaction status endpoint."""

    dl_resp = client.post(
        "/api/v2/bulk_download/transactions",
        content_type="application/json",
        data=json.dumps({"filters": {}, "columns": []}),
    )

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] > 100


def test_download_awards_with_all_prime_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": ["contracts", "direct_payments", "grants", "idvs", "loans", "other_financial_assistance"],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3  # 2 awards, but 1 file with 2 rows and 1 file with 1
    assert resp.json()["total_columns"] == 366


def test_download_awards_with_some_prime_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": ["contracts", "direct_payments", "idvs"],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 2
    assert resp.json()["total_columns"] == 366


def test_download_awards_with_all_sub_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "sub_award_types": ["procurement", "grant"],
        "date_type": "action_date",
        "date_range": {"start_date": "2017-10-01", "end_date": "2018-09-30"},
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3  # 2 awards, but 1 file with 2 rows and 1 file with 1
    assert resp.json()["total_columns"] == 179


def test_download_awards_with_some_sub_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "sub_award_types": ["grant"],
        "date_type": "action_date",
        "date_range": {"start_date": "2017-10-01", "end_date": "2018-09-30"},
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 2
    assert resp.json()["total_columns"] == 89


@pytest.mark.django_db
def test_download_status_nonexistent_file_404(client):
    """Requesting status of nonexistent file should produce HTTP 404"""

    resp = client.get("/api/v2/bulk_download/status/?file_name=there_is_no_such_file.zip")

    assert resp.status_code == status.HTTP_404_NOT_FOUND


def sort_function(agency):
    return agency["toptier_code"]


@pytest.mark.skip
def test_list_agencies(client, award_data):
    """Test transaction list agencies endpoint"""
    resp = client.post("/api/v2/bulk_download/list_agencies", content_type="application/json", data=json.dumps({}))

    all_toptiers = [
        {"name": "Bureau of Things", "toptier_code": "100"},
        {"name": "Bureau of Stuff", "toptier_code": "101"},
        {"name": "Bureau of Money", "toptier_code": "102"},
    ]

    index = 0
    agency_ids = []
    for toptier in sorted(resp.json()["agencies"]["other_agencies"], key=sort_function):
        assert toptier["name"] == all_toptiers[index]["name"]
        assert toptier["toptier_code"] == all_toptiers[index]["toptier_code"]
        agency_ids.append(toptier["toptier_agency_id"])
        index += 1
    assert resp.json()["sub_agencies"] == []
    assert resp.json()["federal_accounts"] == []

    resp = client.post(
        "/api/v2/bulk_download/list_agencies",
        content_type="application/json",
        data=json.dumps({"agency": agency_ids[0]}),
    )

    assert resp.json()["agencies"] == []
    assert resp.json()["sub_agencies"] == [{"subtier_agency_name": "Bureau of Things", "subtier_agency_id": 1}]
    assert resp.json()["federal_accounts"] == []

    resp = client.post(
        "/api/v2/bulk_download/list_agencies",
        content_type="application/json",
        data=json.dumps({"agency": agency_ids[1]}),
    )

    assert resp.json()["agencies"] == []
    assert resp.json()["sub_agencies"] == [{"subtier_agency_name": "Bureau of Stuff", "subtier_agency_id": 2}]
    assert resp.json()["federal_accounts"] == []

    resp = client.post(
        "/api/v2/bulk_download/list_agencies",
        content_type="application/json",
        data=json.dumps({"agency": agency_ids[2]}),
    )

    assert resp.json()["agencies"] == []
    assert resp.json()["sub_agencies"] == [{"subtier_agency_name": "Bureau of Things", "subtier_agency_id": 3}]
    assert resp.json()["federal_accounts"] == [
        {"federal_account_name": "Compensation to Accounts", "federal_account_id": 1}
    ]
