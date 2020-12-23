import json
import pytest
import random

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def download_test_data(db):
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
    mommy.make("references.SubtierAgency", name="Bureau of Things")

    # Create Awarding Agencies
    aa1 = mommy.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = mommy.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    ata3 = mommy.make(
        "references.ToptierAgency",
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding SUB
    mommy.make("references.SubtierAgency", name="Bureau of Things")

    # Create Funding Agency
    mommy.make("references.Agency", id=3, toptier_agency=ata3, toptier_flag=False)

    # Create Awards
    award1 = mommy.make("awards.Award", id=123, category="idv")
    award2 = mommy.make("awards.Award", id=456, category="contracts")
    award3 = mommy.make("awards.Award", id=789, category="assistance")

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa1,
    )
    trann2 = mommy.make(
        TransactionNormalized,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
    )
    trann3 = mommy.make(
        TransactionNormalized,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
    )

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid="tc1piid")
    mommy.make(TransactionFPDS, transaction=trann2, piid="tc2piid")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain="ta1fain")

    # Set latest_award for each award
    update_awards()


def test_download_awards_without_columns(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": []}, "columns": []}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_tsv_download_awards_without_columns(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": []}, "columns": [], "file_format": "tsv"}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_pstxt_download_awards_without_columns(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": []}, "columns": [], "file_format": "pstxt"}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_download_awards_with_columns(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": []},
                "columns": [
                    "total_obligated_amount",
                    "product_or_service_code",
                    "product_or_service_code_description",
                    "naics_code",
                    "naics_description",
                ],
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_download_awards_bad_filter_type_raises(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    payload = {"filters": "01", "columns": []}
    resp = client.post("/api/v2/download/awards/", content_type="application/json", data=json.dumps(payload))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Filters parameter not provided as a dict"
