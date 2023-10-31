import json
import pytest
import random

from model_bakery import baker
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.models import TransactionSearch


@pytest.fixture
def download_test_data(db):
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
    baker.make("references.SubtierAgency", name="Bureau of Things", _fill_optional=True)

    # Create Awarding Agencies
    aa1 = baker.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=False, _fill_optional=True)
    aa2 = baker.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=False, _fill_optional=True)

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
    baker.make("references.SubtierAgency", name="Bureau of Things", _fill_optional=True)

    # Create Funding Agency
    baker.make("references.Agency", id=3, toptier_agency=ata3, toptier_flag=False, _fill_optional=True)

    # Create Awards
    award1 = baker.make(
        "search.AwardSearch",
        award_id=123,
        piid="tc1piid",
        category="idv",
        type="IDV_A",
        action_date="2018-01-01",
        awarding_agency_id=aa1.id,
        generated_unique_award_id="CONT_IDV_tc1piid_123",
    )
    award2 = baker.make(
        "search.AwardSearch",
        award_id=456,
        piid="tc2piid",
        category="contracts",
        type="A",
        action_date="2018-01-01",
        awarding_agency_id=aa2.id,
        generated_unique_award_id="CONT_AWD_tc2piid_456",
    )
    award3 = baker.make(
        "search.AwardSearch",
        award_id=789,
        fain="ta1fain",
        category="assistance",
        type="02",
        action_date="2018-01-01",
        awarding_agency_id=aa2.id,
        generated_unique_award_id="ASST_NON_ta1fain_456",
    )

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


@pytest.mark.django_db
def test_download_assistance_without_columns(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    resp = client.post(
        "/api/v2/download/assistance/",
        content_type="application/json",
        data=json.dumps({"award_id": 789}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db
def test_download_assistance_with_columns(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    resp = client.post(
        "/api/v2/download/assistance/",
        content_type="application/json",
        data=json.dumps(
            {"award_id": 789, "columns": ["prime_award_unique_key", "prime_award_amount", "program_activity_name"]}
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


@pytest.mark.django_db
def test_download_assistance_bad_award_id_raises(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
    payload = {"award_id": -1}
    resp = client.post("/api/v2/download/assistance/", content_type="application/json", data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Unable to find award matching the provided award id"
