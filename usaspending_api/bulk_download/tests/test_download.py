import json

from model_mommy import mommy
import pytest
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards


@pytest.fixture
def award_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Locations
    mommy.make("references.Location")

    # Create LE
    mommy.make("references.LegalEntity")

    # Create Awarding Top Agency
    ata1 = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Things",
        cgac_code="100",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )
    ata2 = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Stuff",
        cgac_code="101",
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
        cgac_code="102",
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
    award1 = mommy.make("awards.Award", category="contracts")
    award2 = mommy.make("awards.Award", category="contracts")
    award3 = mommy.make("awards.Award", category="assistance")

    # Create Transactions
    trann1 = mommy.make(TransactionNormalized, award=award1, modification_number=1, awarding_agency=aa1)
    trann2 = mommy.make(TransactionNormalized, award=award2, modification_number=1, awarding_agency=aa2)
    trann3 = mommy.make(TransactionNormalized, award=award3, modification_number=1, awarding_agency=aa2)

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid="tc1piid")
    mommy.make(TransactionFPDS, transaction=trann2, piid="tc2piid")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain="ta1fain")

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_v2_endpoint(client, award_data):
    """test the transaction endpoint."""

    resp = client.post(
        "/api/v2/bulk_download/transactions",
        content_type="application/json",
        data=json.dumps({"filters": {}, "columns": {}}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
@pytest.mark.skip
def test_download_awards_v2_endpoint(client, award_data):
    """test the awards endpoint."""

    resp = client.post(
        "/api/v2/bulk_download/awards", content_type="application/json", data=json.dumps({"filters": {}, "columns": []})
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
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


@pytest.mark.django_db
@pytest.mark.skip
def test_download_awards_v2_status_endpoint(client, award_data):
    """Test the transaction status endpoint."""

    dl_resp = client.post(
        "/api/v2/bulk_download/awards", content_type="application/json", data=json.dumps({"filters": {}, "columns": []})
    )

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3  # 2 awards, but 1 file with 2 rows and 1 file with 1``0`
    assert resp.json()["total_columns"] > 100


@pytest.mark.django_db
@pytest.mark.skip
def test_download_status_nonexistent_file_404(client):
    """Requesting status of nonexistent file should produce HTTP 404"""

    resp = client.get("/api/v2/bulk_download/status/?file_name=there_is_no_such_file.zip")

    assert resp.status_code == status.HTTP_404_NOT_FOUND


def sort_function(agency):
    return agency["cgac_code"]


@pytest.mark.skip
def test_list_agencies(client, award_data):
    """Test transaction list agencies endpoint"""
    resp = client.post("/api/v2/bulk_download/list_agencies", content_type="application/json", data=json.dumps({}))

    all_toptiers = [
        {"name": "Bureau of Things", "cgac_code": "100"},
        {"name": "Bureau of Stuff", "cgac_code": "101"},
        {"name": "Bureau of Money", "cgac_code": "102"},
    ]

    index = 0
    agency_ids = []
    for toptier in sorted(resp.json()["agencies"]["other_agencies"], key=sort_function):
        assert toptier["name"] == all_toptiers[index]["name"]
        assert toptier["cgac_code"] == all_toptiers[index]["cgac_code"]
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
