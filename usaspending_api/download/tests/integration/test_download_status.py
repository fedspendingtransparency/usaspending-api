import json
import pytest
import random

from unittest.mock import Mock

from django.conf import settings
from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.models import TransactionSearch
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def download_test_data(transactional_db):
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
        category="idv",
        generated_unique_award_id="CONT_IDV_NEW",
        action_date="2020-01-01",
    )
    award2 = baker.make(
        "search.AwardSearch",
        award_id=456,
        category="contracts",
        generated_unique_award_id="CONT_AWD_NEW",
        action_date="2020-01-01",
    )
    award3 = baker.make(
        "search.AwardSearch",
        award_id=789,
        category="assistance",
        generated_unique_award_id="ASST_NON_NEW",
        action_date="2020-01-01",
    )

    # Create Transactions
    baker.make(
        TransactionSearch,
        transaction_id=1,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa1.id,
        generated_unique_award_id="CONT_IDV_NEW",
        is_fpds=True,
        piid="tc1piid",
        awarding_toptier_agency_name="Bureau of Things",
    )
    baker.make(
        TransactionSearch,
        transaction_id=2,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="CONT_AWD_NEW",
        is_fpds=True,
        piid="tc2piid",
        awarding_toptier_agency_name="Bureau of Stuff",
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        generated_unique_award_id="ASST_NON_NEW",
        is_fpds=False,
        fain="ta1fain",
        awarding_toptier_agency_name="Bureau of Stuff",
    )

    # Set latest_award for each award
    update_awards()


def get_number_of_columns_for_query_paths(*download_tuples):
    count = 0
    for download_tuple in download_tuples:
        model_type = download_tuple[0]
        file_type = download_tuple[1]
        count += len(query_paths[model_type][file_type])
    return count


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_assistance_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/assistance/",
        content_type="application/json",
        data=json.dumps({"award_id": 789}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award_financial", "treasury_account"), ("assistance_transaction_history", "d2"), ("subaward_search", "d2")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == expected_number_of_columns

    # Test with columns specified
    dl_resp = client.post(
        "/api/v2/download/assistance/",
        content_type="application/json",
        data=json.dumps(
            {
                "award_id": 789,
                "columns": [
                    "assistance_transaction_unique_key",
                    "prime_award_unique_key",
                    "prime_award_amount",
                    "program_activity_name",
                ],
            }
        ),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == 4


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_awards_status(
    client, download_test_data, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": list(award_type_mapping.keys())}, "columns": []}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award", "d1"), ("award", "d2"), ("subaward_search", "d1"), ("subaward_search", "d2")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] == expected_number_of_columns

    # Test with columns specified
    dl_resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": list(award_type_mapping.keys())},
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
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] == 6


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_contract_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/contract/", content_type="application/json", data=json.dumps({"award_id": 456})
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award_financial", "treasury_account"), ("idv_transaction_history", "d1"), ("subaward_search", "d1")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == expected_number_of_columns

    # Test with columns specified
    dl_resp = client.post(
        "/api/v2/download/contract/",
        content_type="application/json",
        data=json.dumps(
            {
                "award_id": 456,
                "columns": [
                    "prime_award_unique_key",
                    "prime_award_amount",
                    "current_total_value_of_award",
                    "contract_award_unique_key",
                    "program_activity_name",
                ],
            }
        ),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == 5


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_idv_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    # Test without columns specified
    dl_resp = client.post("/api/v2/download/idv/", content_type="application/json", data=json.dumps({"award_id": 123}))
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award_financial", "treasury_account"), ("idv_orders", "d1"), ("idv_transaction_history", "d1")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == expected_number_of_columns

    # Test with columns specified
    dl_resp = client.post(
        "/api/v2/download/idv/",
        content_type="application/json",
        data=json.dumps(
            {
                "award_id": 123,
                "columns": ["current_total_value_of_award", "contract_award_unique_key", "program_activity_name"],
            }
        ),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 1
    assert resp.json()["total_columns"] == 5


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_transactions_status(
    client, download_test_data, monkeypatch, elasticsearch_transaction_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"agencies": [{"type": "awarding", "tier": "toptier", "name": "Bureau of Stuff"}]},
                "columns": [],
            }
        ),
    )

    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("transaction_search", "d1"), ("transaction_search", "d2"), ("subaward_search", "d1"), ("subaward_search", "d2")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 2
    assert resp.json()["total_columns"] == expected_number_of_columns

    # Test with columns specified
    dl_resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "agencies": [
                        {"type": "awarding", "tier": "toptier", "name": "Bureau of Stuff"},
                        {"type": "awarding", "tier": "toptier", "name": "Bureau of Things"},
                    ]
                },
                "columns": ["award_id_piid", "modification_number"],
            }
        ),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] == 3


@pytest.mark.django_db(databases=[settings.DOWNLOAD_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_download_transactions_limit(
    client, download_test_data, monkeypatch, elasticsearch_transaction_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    dl_resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps({"limit": 1, "filters": {"award_type_codes": list(award_type_mapping.keys())}, "columns": []}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("transaction_search", "d1"), ("transaction_search", "d2"), ("subaward_search", "d1"), ("subaward_search", "d2")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 2
    assert resp.json()["total_columns"] == expected_number_of_columns
