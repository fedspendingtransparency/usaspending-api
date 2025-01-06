import json
import pytest

from unittest.mock import Mock

from django.conf import settings
from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import all_subaward_types, award_type_mapping
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.models import SubawardSearch, TransactionSearch
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def _award_download_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Awarding Top Agency
    ata1 = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=1,
        name="Bureau of Things",
        toptier_code="100",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )
    ata2 = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2,
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Awarding subs
    asa1 = baker.make("references.SubtierAgency", name="SubBureau of Things", _fill_optional=True)
    asa2 = baker.make("references.SubtierAgency", name="SubBureau of Stuff", _fill_optional=True)

    # Create Awarding Agencies
    aa1 = baker.make(
        "references.Agency", toptier_agency=ata1, subtier_agency=asa1, toptier_flag=False, user_selectable=True
    )
    aa2 = baker.make(
        "references.Agency", toptier_agency=ata2, subtier_agency=asa2, toptier_flag=False, user_selectable=True
    )

    # Create Funding Top Agency
    fta = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=3,
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding SUB
    fsa1 = baker.make("references.SubtierAgency", name="Bureau of Things", _fill_optional=True)

    # Create Funding Agency
    baker.make("references.Agency", toptier_agency=fta, subtier_agency=fsa1, toptier_flag=False, _fill_optional=True)

    # Create Federal Account
    baker.make("accounts.FederalAccount", account_title="Compensation to Accounts", agency_identifier="102", id=1)

    # Create Awards
    baker.make("search.AwardSearch", award_id=1, category="contracts", generated_unique_award_id="TEST_AWARD_1")
    baker.make("search.AwardSearch", award_id=2, category="contracts", generated_unique_award_id="TEST_AWARD_2")
    baker.make("search.AwardSearch", award_id=3, category="assistance", generated_unique_award_id="TEST_AWARD_3")
    baker.make("search.AwardSearch", award_id=4, category="contracts", generated_unique_award_id="TEST_AWARD_4")
    baker.make("search.AwardSearch", award_id=5, category="assistance", generated_unique_award_id="TEST_AWARD_5")
    baker.make("search.AwardSearch", award_id=6, category="assistance", generated_unique_award_id="TEST_AWARD_6")
    baker.make("search.AwardSearch", award_id=7, category="contracts", generated_unique_award_id="TEST_AWARD_7")
    baker.make("search.AwardSearch", award_id=8, category="assistance", generated_unique_award_id="TEST_AWARD_8")
    baker.make("search.AwardSearch", award_id=9, category="assistance", generated_unique_award_id="TEST_AWARD_9")

    # Create Transactions
    baker.make(
        TransactionSearch,
        transaction_id=1,
        award_id=1,
        modification_number=1,
        awarding_agency_id=aa1.id,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2017-01-01",
        type="A",
        is_fpds=True,
        piid="tc1piid",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    baker.make(
        TransactionSearch,
        transaction_id=2,
        award_id=2,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_2",
        action_date="2017-04-01",
        type="IDV_B",
        is_fpds=True,
        piid="tc2piid",
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        pop_country_code="CAN",
        pop_country_name="CANADA",
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        award_id=3,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_3",
        action_date="2017-06-01",
        type="02",
        is_fpds=False,
        fain="ta1fain",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    baker.make(
        TransactionSearch,
        transaction_id=4,
        award_id=4,
        modification_number=1,
        awarding_agency_id=aa1.id,
        generated_unique_award_id="TEST_AWARD_4",
        action_date="2018-01-15",
        type="A",
        is_fpds=True,
        piid="tc4piid",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    baker.make(
        TransactionSearch,
        transaction_id=5,
        award_id=5,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_5",
        action_date="2018-03-15",
        type="07",
        is_fpds=False,
        fain="ta5fain",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    baker.make(
        TransactionSearch,
        transaction_id=6,
        award_id=6,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_6",
        action_date="2018-06-15",
        type="02",
        is_fpds=False,
        fain="ta6fain",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
    )
    baker.make(
        TransactionSearch,
        transaction_id=7,
        award_id=7,
        modification_number=1,
        awarding_agency_id=aa1.id,
        generated_unique_award_id="TEST_AWARD_7",
        action_date="2017-01-15",
        type="A",
        is_fpds=True,
        piid="tc7piid",
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        pop_country_code="CAN",
        pop_country_name="CANADA",
    )
    baker.make(
        TransactionSearch,
        transaction_id=8,
        award_id=8,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_8",
        action_date="2017-03-15",
        type="07",
        is_fpds=False,
        fain="ta8fain",
        recipient_location_country_code="USA",
        pop_country_code="USA",
    )
    baker.make(
        TransactionSearch,
        transaction_id=9,
        award_id=9,
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="TEST_AWARD_9",
        action_date="2017-06-15",
        type="02",
        is_fpds=False,
        fain="ta9fain",
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        pop_country_code="CAN",
        pop_country_name="CANADA",
    )

    # Create SubawardSearch
    baker.make(
        SubawardSearch,
        broker_subaward_id=1,
        award_id=4,
        latest_transaction_id=4,
        action_date="2018-01-15",
        sub_action_date="2018-01-15",
        prime_award_group="procurement",
        sub_legal_entity_country_code_raw="USA",
        sub_legal_entity_country_name="UNITED STATES",
        sub_place_of_perform_country_co_raw="USA",
        sub_place_of_perform_country_name="UNITED STATES",
        subaward_type="sub-contract",
    )
    baker.make(
        SubawardSearch,
        broker_subaward_id=2,
        award_id=5,
        latest_transaction_id=5,
        action_date="2018-03-15",
        sub_action_date="2018-03-15",
        prime_award_group="grant",
        sub_legal_entity_country_code_raw="USA",
        sub_legal_entity_country_name="UNITED STATES",
        sub_place_of_perform_country_co_raw="USA",
        sub_place_of_perform_country_name="UNITED STATES",
        subaward_type="sub-grant",
    )
    baker.make(
        SubawardSearch,
        broker_subaward_id=3,
        award_id=6,
        latest_transaction_id=6,
        action_date="2018-06-15",
        sub_action_date="2018-06-15",
        prime_award_group="grant",
        sub_legal_entity_country_code_raw="USA",
        sub_legal_entity_country_name="UNITED STATES",
        sub_place_of_perform_country_co_raw="USA",
        sub_place_of_perform_country_name="UNITED STATES",
        subaward_type="sub-grant",
    )
    baker.make(
        SubawardSearch,
        broker_subaward_id=4,
        award_id=7,
        latest_transaction_id=7,
        action_date="2017-01-15",
        sub_action_date="2017-01-15",
        prime_award_group="procurement",
        sub_legal_entity_country_code_raw="USA",
        sub_legal_entity_country_name="UNITED STATES",
        sub_place_of_perform_country_co_raw="USA",
        sub_place_of_perform_country_name="UNITED STATES",
        subaward_type="sub-contract",
    )
    baker.make(
        SubawardSearch,
        broker_subaward_id=5,
        award_id=8,
        latest_transaction_id=8,
        action_date="2017-03-15",
        sub_action_date="2017-03-15",
        prime_award_group="grant",
        sub_legal_entity_country_code_raw="CAN",
        sub_legal_entity_country_name="CANADA",
        sub_place_of_perform_country_co_raw="CAN",
        sub_place_of_perform_country_name="CANADA",
        subaward_type="sub-grant",
    )
    baker.make(
        SubawardSearch,
        broker_subaward_id=6,
        award_id=9,
        latest_transaction_id=9,
        action_date="2017-06-15",
        sub_action_date="2017-06-15",
        prime_award_group="grant",
        sub_legal_entity_country_code_raw="CAN",
        sub_legal_entity_country_name="CANADA",
        sub_place_of_perform_country_co_raw="CAN",
        sub_place_of_perform_country_name="CANADA",
        subaward_type="sub-grant",
    )

    # Ref Country Code
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    baker.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_all_award_types(client, monkeypatch, _award_download_data, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 640


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_all_prime_awards(client, _award_download_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 409


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_some_prime_awards(client, _award_download_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 297


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_all_sub_awards(client, monkeypatch, _award_download_data, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 231


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_some_sub_awards(client, monkeypatch, _award_download_data, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 113


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_domestic_scope(client, monkeypatch, _award_download_data, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    # Recipient Location Scope
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 640

    # Place of Performance Scope
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 640


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_with_foreign_scope(client, monkeypatch, _award_download_data, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    # Recipient Location Scope
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 640

    # Place of Performance Scope
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())
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
    assert resp.json()["total_columns"] == 640


@pytest.mark.django_db
def test_download_status_nonexistent_file_404(client):
    """Requesting status of nonexistent file should produce HTTP 404"""

    resp = client.get("/api/v2/bulk_download/status/?file_name=there_is_no_such_file.zip")

    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_list_agencies(client, _award_download_data):
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


@pytest.mark.django_db
def test_empty_array_filter_fail(client, _award_download_data):
    filters = {
        "agency": "all",
        "prime_award_types": [*list(award_type_mapping.keys())],
        "sub_award_types": [],
        "date_type": "action_date",
        "date_range": {"start_date": "2016-10-01", "end_date": "2017-09-30"},
        "recipient_scope": "foreign",
    }
    resp = client.post(
        "/api/v2/bulk_download/awards", content_type="application/json", data=json.dumps({"filters": filters})
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert (
        "Field 'filters|sub_award_types' value '[]' is below min '1' items" in resp.json()["detail"]
    ), "Incorrect error message"
