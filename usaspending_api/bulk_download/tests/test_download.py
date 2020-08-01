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
from usaspending_api.awards.v2.lookups.lookups import all_subaward_types, award_type_mapping
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
        toptier_agency_id=1,
        name="Bureau of Things",
        toptier_code="100",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )
    ata2 = mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=2,
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Awarding subs
    asa1 = mommy.make("references.SubtierAgency", name="SubBureau of Things")
    asa2 = mommy.make("references.SubtierAgency", name="SubBureau of Stuff")

    # Create Awarding Agencies
    aa1 = mommy.make(
        "references.Agency", toptier_agency=ata1, subtier_agency=asa1, toptier_flag=False, user_selectable=True
    )
    aa2 = mommy.make(
        "references.Agency", toptier_agency=ata2, subtier_agency=asa2, toptier_flag=False, user_selectable=True
    )

    # Create Funding Top Agency
    fta = mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=3,
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
    mommy.make("awards.Award", id=7, category="contracts", generated_unique_award_id="TEST_AWARD_7")
    mommy.make("awards.Award", id=8, category="assistance", generated_unique_award_id="TEST_AWARD_8")
    mommy.make("awards.Award", id=9, category="assistance", generated_unique_award_id="TEST_AWARD_9")

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
        is_fpds=True,
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
        is_fpds=True,
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
        is_fpds=False,
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
        is_fpds=True,
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
        is_fpds=False,
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
        is_fpds=False,
    )
    mommy.make(
        TransactionNormalized,
        id=7,
        award_id=7,
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="TEST_AWARD_7",
        action_date="2017-01-15",
        type="A",
        is_fpds=True,
    )
    mommy.make(
        TransactionNormalized,
        id=8,
        award_id=8,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_8",
        action_date="2017-03-15",
        type="07",
        is_fpds=False,
    )
    mommy.make(
        TransactionNormalized,
        id=9,
        award_id=9,
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="TEST_AWARD_9",
        action_date="2017-06-15",
        type="02",
        is_fpds=False,
    )

    # Create TransactionContract
    mommy.make(
        TransactionFPDS,
        transaction_id=1,
        piid="tc1piid",
        unique_award_key="TEST_AWARD_1",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_c="USA",
        place_of_perf_country_desc="UNITED STATES",
    )
    mommy.make(
        TransactionFPDS,
        transaction_id=2,
        piid="tc2piid",
        unique_award_key="TEST_AWARD_2",
        legal_entity_country_code="CAN",
        legal_entity_country_name="CANADA",
        place_of_perform_country_c="CAN",
        place_of_perf_country_desc="CANADA",
    )
    mommy.make(
        TransactionFPDS,
        transaction_id=4,
        piid="tc4piid",
        unique_award_key="TEST_AWARD_4",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_c="USA",
        place_of_perf_country_desc="UNITED STATES",
    )
    mommy.make(
        TransactionFPDS,
        transaction_id=7,
        piid="tc7piid",
        unique_award_key="TEST_AWARD_7",
        legal_entity_country_code="CAN",
        legal_entity_country_name="CANADA",
        place_of_perform_country_c="CAN",
        place_of_perf_country_desc="CANADA",
    )

    # Create TransactionAssistance
    mommy.make(
        TransactionFABS,
        transaction_id=3,
        fain="ta1fain",
        unique_award_key="TEST_AWARD_3",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
    )
    mommy.make(
        TransactionFABS,
        transaction_id=5,
        fain="ta5fain",
        unique_award_key="TEST_AWARD_5",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
    )
    mommy.make(
        TransactionFABS,
        transaction_id=6,
        fain="ta6fain",
        unique_award_key="TEST_AWARD_6",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
    )
    mommy.make(
        TransactionFABS,
        transaction_id=8,
        fain="ta8fain",
        unique_award_key="TEST_AWARD_8",
        legal_entity_country_code="USA",
        place_of_perform_country_c="USA",
    )
    mommy.make(
        TransactionFABS,
        transaction_id=9,
        fain="ta9fain",
        unique_award_key="TEST_AWARD_9",
        legal_entity_country_code="CAN",
        legal_entity_country_name="CANADA",
        place_of_perform_country_c="CAN",
        place_of_perform_country_n="CANADA",
    )

    # Create Subaward
    mommy.make(
        Subaward,
        id=1,
        award_id=4,
        latest_transaction_id=4,
        action_date="2018-01-15",
        award_type="procurement",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    mommy.make(
        Subaward,
        id=2,
        award_id=5,
        latest_transaction_id=5,
        action_date="2018-03-15",
        award_type="grant",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    mommy.make(
        Subaward,
        id=3,
        award_id=6,
        latest_transaction_id=6,
        action_date="2018-06-15",
        award_type="grant",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    mommy.make(
        Subaward,
        id=4,
        award_id=7,
        latest_transaction_id=7,
        action_date="2017-01-15",
        award_type="procurement",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    mommy.make(
        Subaward,
        id=5,
        award_id=8,
        latest_transaction_id=8,
        action_date="2017-03-15",
        award_type="grant",
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        pop_country_code="CAN",
        pop_country_name="CANADA",
    )
    mommy.make(
        Subaward,
        id=6,
        award_id=9,
        latest_transaction_id=9,
        action_date="2017-06-15",
        award_type="grant",
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        pop_country_code="CAN",
        pop_country_name="CANADA",
    )

    # Create BrokerSubaward
    mommy.make(
        BrokerSubaward,
        id=1,
        prime_id=4,
        action_date="2018-01-15",
        subaward_type="sub-contract",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_co="USA",
        place_of_perform_country_na="UNITED STATES",
    )
    mommy.make(
        BrokerSubaward,
        id=2,
        prime_id=5,
        action_date="2018-03-15",
        subaward_type="sub-grant",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_co="USA",
        place_of_perform_country_na="UNITED STATES",
    )
    mommy.make(
        BrokerSubaward,
        id=3,
        prime_id=6,
        action_date="2018-06-15",
        subaward_type="sub-grant",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_co="USA",
        place_of_perform_country_na="UNITED STATES",
    )
    mommy.make(
        BrokerSubaward,
        id=4,
        prime_id=7,
        action_date="2017-01-15",
        subaward_type="sub-contract",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        place_of_perform_country_co="USA",
        place_of_perform_country_na="UNITED STATES",
    )
    mommy.make(
        BrokerSubaward,
        id=5,
        prime_id=8,
        action_date="2017-03-15",
        subaward_type="sub-grant",
        legal_entity_country_code="CAN",
        legal_entity_country_name="CANADA",
        place_of_perform_country_co="CAN",
        place_of_perform_country_na="CANADA",
    )
    mommy.make(
        BrokerSubaward,
        id=6,
        prime_id=9,
        action_date="2017-06-15",
        subaward_type="sub-grant",
        legal_entity_country_code="CAN",
        legal_entity_country_name="CANADA",
        place_of_perform_country_co="CAN",
        place_of_perform_country_na="CANADA",
    )

    # Ref Country Code
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    mommy.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")

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


def test_download_awards_with_all_award_types(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [*all_subaward_types],
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
    assert resp.json()["total_rows"] == 9
    assert resp.json()["total_columns"] == 579


def test_download_awards_with_all_prime_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": list(award_type_mapping.keys()),
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
    assert resp.json()["total_rows"] == 6
    assert resp.json()["total_columns"] == 377


def test_download_awards_with_some_prime_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": ["A", "IDV_B"],
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
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] == 282


def test_download_awards_with_all_sub_awards(client, award_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "sub_award_types": all_subaward_types,
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
    assert resp.json()["total_columns"] == 202


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
    assert resp.json()["total_columns"] == 99


def test_download_awards_with_domestic_scope(client, award_data):
    # Recipient Location Scope
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [*all_subaward_types],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
        "recipient_scope": "domestic",
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 4
    assert resp.json()["total_columns"] == 579

    # Place of Performance Scope
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [*all_subaward_types],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
        "place_of_performance_scope": "domestic",
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 4
    assert resp.json()["total_columns"] == 579


def test_download_awards_with_foreign_scope(client, award_data):
    # Recipient Location Scope
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [*all_subaward_types],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
        "recipient_scope": "foreign",
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 5
    assert resp.json()["total_columns"] == 579

    # Place of Performance Scope
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [*all_subaward_types],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
        "place_of_performance_scope": "foreign",
    }
    dl_resp = client.post(
        "/api/v2/bulk_download/awards",
        content_type="application/json",
        data=json.dumps({"filters": filters, "columns": []}),
    )
    assert dl_resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 5
    assert resp.json()["total_columns"] == 579


@pytest.mark.django_db
def test_download_status_nonexistent_file_404(client):
    """Requesting status of nonexistent file should produce HTTP 404"""

    resp = client.get("/api/v2/bulk_download/status/?file_name=there_is_no_such_file.zip")

    assert resp.status_code == status.HTTP_404_NOT_FOUND


def test_list_agencies(client, award_data):
    """Test transaction list agencies endpoint"""
    resp = client.post(
        "/api/v2/bulk_download/list_agencies",
        content_type="application/json",
        data=json.dumps({"type": "award_agencies"}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "agencies": {
            "cfo_agencies": [],
            "other_agencies": [
                {"name": "Bureau of Stuff", "toptier_agency_id": 2, "toptier_code": "101"},
                {"name": "Bureau of Things", "toptier_agency_id": 1, "toptier_code": "100"},
            ],
        },
        "sub_agencies": [],
    }

    resp = client.post(
        "/api/v2/bulk_download/list_agencies",
        content_type="application/json",
        data=json.dumps({"type": "award_agencies", "agency": 2}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {"agencies": [], "sub_agencies": [{"subtier_agency_name": "SubBureau of Stuff"}]}
