import json
import pytest
import random

from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def download_test_data(transactional_db):
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
    award1 = mommy.make("awards.Award", id=123, category="idv", generated_unique_award_id="CONT_IDV_NEW")
    award2 = mommy.make("awards.Award", id=456, category="contracts", generated_unique_award_id="CONT_AWD_NEW")
    award3 = mommy.make("awards.Award", id=789, category="assistance", generated_unique_award_id="ASST_NON_NEW")

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        id=1,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="CONT_IDV_NEW",
    )
    trann2 = mommy.make(
        TransactionNormalized,
        id=2,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="CONT_AWD_NEW",
    )
    trann3 = mommy.make(
        TransactionNormalized,
        id=3,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="ASST_NON_NEW",
    )

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid="tc1piid", unique_award_key="CONT_IDV_NEW")
    mommy.make(TransactionFPDS, transaction=trann2, piid="tc2piid", unique_award_key="CONT_AWD_NEW")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain="ta1fain", unique_award_key="ASST_NON_NEW")

    # Set latest_award for each award
    update_awards()


def get_number_of_columns_for_query_paths(*download_tuples):
    count = 0
    for download_tuple in download_tuples:
        model_type = download_tuple[0]
        file_type = download_tuple[1]
        count += len(query_paths[model_type][file_type])
    return count


def test_download_assistance_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/assistance/",
        content_type="application/json",
        data=json.dumps({"award_id": 789, "columns": []}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award_financial", "treasury_account"), ("assistance_transaction_history", "d2"), ("subaward", "d2")
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


def test_download_awards_status(client, download_test_data, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/awards/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": []}, "columns": []}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award", "d1"), ("award", "d2"), ("subaward", "d1"), ("subaward", "d2")
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
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 3
    assert resp.json()["total_columns"] == 6


def test_download_contract_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/contract/", content_type="application/json", data=json.dumps({"award_id": 456, "columns": []})
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("award_financial", "treasury_account"),
        ("idv_transaction_history", "d1"),
        ("subaward", "d1"),
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


def test_download_idv_status(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    # Test without columns specified
    dl_resp = client.post(
        "/api/v2/download/idv/", content_type="application/json", data=json.dumps({"award_id": 123, "columns": []})
    )
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


def test_download_transactions_status(client, download_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

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
        ("transaction", "d1"), ("transaction", "d2"), ("subaward", "d1"), ("subaward", "d2")
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


def test_download_transactions_limit(client, download_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    dl_resp = client.post(
        "/api/v2/download/transactions/",
        content_type="application/json",
        data=json.dumps({"limit": 1, "filters": {"award_type_codes": []}, "columns": []}),
    )
    resp = client.get("/api/v2/download/status/?file_name={}".format(dl_resp.json()["file_name"]))

    expected_number_of_columns = get_number_of_columns_for_query_paths(
        ("transaction", "d1"), ("transaction", "d2"), ("subaward", "d1"), ("subaward", "d2")
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["total_rows"] == 2
    assert resp.json()["total_columns"] == expected_number_of_columns
