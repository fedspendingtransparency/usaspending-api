import json
import pytest
import random

from django.conf import settings
from model_bakery import baker
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.models import TransactionSearch


@pytest.fixture
def download_test_data():
    # Populate job status lookup table
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Awarding Top Agency
    ata1 = baker.make(
        "references.ToptierAgency",
        name="Bureau of Things",
        toptier_code="100",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )
    ata2 = baker.make(
        "references.ToptierAgency",
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Awarding subs
    baker.make("references.SubtierAgency", name="Bureau of Things")

    # Create Awarding Agencies
    aa1 = baker.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = baker.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    ata3 = baker.make(
        "references.ToptierAgency",
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding SUB
    baker.make("references.SubtierAgency", name="Bureau of Things")

    # Create Funding Agency
    baker.make("references.Agency", id=3, toptier_agency=ata3, toptier_flag=False)

    # Create Awards
    award1 = baker.make("search.AwardSearch", award_id=123, category="idv", action_date="2020-01-01")
    award2 = baker.make("search.AwardSearch", award_id=456, category="contracts", action_date="2020-01-01")
    award3 = baker.make("search.AwardSearch", award_id=789, category="assistance", action_date="2020-01-01")

    # Create Transactions
    baker.make(
        TransactionSearch,
        transaction_id=1,
        is_fpds=True,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa1.id,
        piid="tc1piid",
    )
    baker.make(
        TransactionSearch,
        transaction_id=2,
        is_fpds=True,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        piid="tc2piid",
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        is_fpds=False,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        fain="ta1fain",
    )

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db(transaction=True)
def test_download_transactions_without_columns(
    client, monkeypatch, download_test_data, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A"]}, "columns": []}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db(transaction=True)
def test_download_transactions_with_columns(client, monkeypatch, download_test_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": list(award_type_mapping.keys())},
                "columns": [
                    "assistance_transaction_unique_key",
                    "award_id_fain",
                    "modification_number",
                    "sai_number",
                    "contract_transaction_unique_key",
                ],
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db(transaction=True)
def test_download_transactions_bad_limit(client, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps({"limit": "wombats", "filters": {"award_type_codes": ["A"]}, "columns": []}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db(transaction=True)
def test_download_transactions_excessive_limit(
    client, monkeypatch, download_test_data, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps(
            {"limit": settings.MAX_DOWNLOAD_LIMIT + 1, "filters": {"award_type_codes": ["A"]}, "columns": []}
        ),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db(transaction=True)
def test_download_transactions_bad_column_list_raises(
    client, monkeypatch, download_test_data, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    payload = {"filters": {"award_type_codes": ["A"]}, "columns": ["modification_number", "bogus_column"]}
    resp = client.post("/api/v2/download/transactions/", content_type="application/json", data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert "Unknown columns" in resp.json()["detail"]
    assert "bogus_column" in resp.json()["detail"]
    assert "modification_number" not in resp.json()["detail"]


@pytest.mark.django_db(transaction=True)
def test_download_transactions_bad_filter_type_raises(
    client, monkeypatch, download_test_data, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    payload = {"filters": "01", "columns": []}
    resp = client.post("/api/v2/download/transactions/", content_type="application/json", data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Filters parameter not provided as a dict"
