import json
import pytest
import random

from model_bakery import baker
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.search.models import TransactionSearch
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


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
        _fill_optional=True,
    )
    ata2 = baker.make(
        "references.ToptierAgency",
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test.com",
        mission="test",
        icon_filename="test",
        _fill_optional=True,
    )

    # Create Awarding subs
    baker.make("references.SubtierAgency", name="Bureau of Things", _fill_optional=True)

    # Create Awarding Agencies
    aa1 = baker.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=True, _fill_optional=True)
    aa2 = baker.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=True, _fill_optional=True)

    # Create Funding Top Agency
    ata3 = baker.make(
        "references.ToptierAgency",
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
        _fill_optional=True,
    )

    # Create Funding SUB
    baker.make("references.SubtierAgency", name="Bureau of Things", _fill_optional=True)

    # Create Funding Agency
    baker.make("references.Agency", id=3, toptier_agency=ata3, toptier_flag=True, _fill_optional=True)

    # Create Awards
    award1 = baker.make("search.AwardSearch", award_id=123, category="idv")
    award2 = baker.make("search.AwardSearch", award_id=456, category="contracts")
    award3 = baker.make("search.AwardSearch", award_id=789, category="assistance")

    # Create Transactions
    baker.make(
        TransactionSearch,
        transaction_id=1,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa1.id,
        is_fpds=True,
        piid="tc1piid",
        awarding_agency_code="100",
        awarding_toptier_agency_name="Bureau of Things",
        awarding_subtier_agency_name="Bureau of Things",
    )
    baker.make(
        TransactionSearch,
        transaction_id=2,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        is_fpds=True,
        piid="tc2piid",
        awarding_agency_code="101",
        awarding_toptier_agency_name="Bureau of Stuff",
        awarding_subtier_agency_name="Bureau of Things",
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        is_fpds=False,
        fain="ta1fain",
        awarding_agency_code="101",
        awarding_toptier_agency_name="Bureau of Stuff",
        awarding_subtier_agency_name="Bureau of Things",
    )

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db(transaction=True)
def test_download_awards_without_columns(
    client, monkeypatch, download_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A"]}, "columns": []}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db(transaction=True)
def test_tsv_download_awards_without_columns(
    client, monkeypatch, download_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A"]}, "columns": [], "file_format": "tsv"}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db(transaction=True)
def test_pstxt_download_awards_without_columns(
    client, monkeypatch, download_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A"]}, "columns": [], "file_format": "pstxt"}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db(transaction=True)
def test_download_awards_with_columns(
    client, monkeypatch, download_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": ["A"]},
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


@pytest.mark.django_db(transaction=True)
def test_download_awards_bad_filter_type_raises(client, monkeypatch, download_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    payload = {"filters": "01", "columns": []}
    resp = client.post("/api/v2/download/awards/", content_type="application/json", data=json.dumps(payload))

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Filters parameter not provided as a dict"
