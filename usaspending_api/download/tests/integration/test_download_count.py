import json
import pytest
import random

from django.conf import settings
from model_bakery import baker
from unittest.mock import Mock

from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
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
        "search.AwardSearch", award_id=123, display_award_id="123", action_date="2018-01-01", category="idv"
    )
    award2 = baker.make(
        "search.AwardSearch", award_id=456, display_award_id="456", action_date="2018-01-02", category="contracts"
    )
    award3 = baker.make(
        "search.AwardSearch", award_id=789, display_award_id="789", action_date="2018-01-03", category="assistance"
    )

    # Create Transactions
    baker.make(
        TransactionSearch,
        transaction_id=1,
        award=award1,
        action_date="2018-01-02",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa1.id,
        is_fpds=True,
        piid="tc1piid",
        awarding_toptier_agency_name="Bureau of Things",
        awarding_subtier_agency_name="Bureau of Things",
    )
    baker.make(
        TransactionSearch,
        transaction_id=2,
        award=award2,
        action_date="2018-01-02",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        is_fpds=True,
        piid="tc2piid",
        awarding_toptier_agency_name="Bureau of Stuff",
        awarding_subtier_agency_name="Bureau of Things",
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        award=award3,
        action_date="2018-01-02",
        award_date_signed="2020-01-02",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency_id=aa2.id,
        is_fpds=False,
        fain="ta1fain",
        awarding_toptier_agency_name="Bureau of Stuff",
        awarding_subtier_agency_name="Bureau of Things",
    )

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db(transaction=True)
def test_download_count(client, download_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(
            {"filters": {"agencies": [{"type": "awarding", "tier": "toptier", "name": "Bureau of Things"}]}}
        ),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp_json["calculated_count"] == 1
    assert resp_json["maximum_limit"] == settings.MAX_DOWNLOAD_LIMIT
    assert resp_json["rows_gt_limit"] is False


@pytest.mark.django_db(transaction=True)
def test_download_count_with_date_type_filter_default(
    client, download_test_data, monkeypatch, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"start_date": "2018-01-01", "end_date": "2018-01-03"}],
                },
                "spending_level": "transactions",
            }
        ),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp_json["calculated_count"] == 3
    assert resp_json["maximum_limit"] == settings.MAX_DOWNLOAD_LIMIT
    assert resp_json["rows_gt_limit"] is False


@pytest.mark.django_db(transaction=True)
def test_messages_not_nested(client, download_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    request_data = {
        "filters": {"time_period": [{"start_date": "2018-01-01", "end_date": "2018-01-03"}], "not_a_real_filter": "abc"}
    }
    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(request_data),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    message_list = get_generic_filters_message(request_data["filters"].keys(), {"time_period", "award_type_codes"})
    message_list.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. See documentation for more information. "
    )
    message_list.append(
        "The above fields containing the transaction_* naming convention will be deprecated and replaced with fields without the transaction_*. "
    )

    assert resp_json["messages"] == message_list


@pytest.mark.django_db(transaction=True)
def test_download_count_with_date_type_filter_date_signed(
    client, download_test_data, monkeypatch, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2018-01-01", "end_date": "2018-01-03"}],
                }
            }
        ),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp_json["calculated_count"] == 0
    assert resp_json["maximum_limit"] == settings.MAX_DOWNLOAD_LIMIT
    assert resp_json["rows_gt_limit"] is False

    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2020-01-01", "end_date": "2020-01-03"}],
                }
            }
        ),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp_json["calculated_count"] == 1
    assert resp_json["maximum_limit"] == settings.MAX_DOWNLOAD_LIMIT
    assert resp_json["rows_gt_limit"] is False


@pytest.mark.django_db(transaction=True)
def test_download_count_with_spending_level(client, download_test_data, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string())

    request_data = {"filters": {"award_ids": ["123", "456", "789"]}, "spending_level": "awards"}
    resp = client.post(
        "/api/v2/download/count/",
        content_type="application/json",
        data=json.dumps(request_data),
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp_json["calculated_count"] == 3
    assert resp_json["spending_level"] == "awards"
    assert resp_json["maximum_limit"] == settings.MAX_DOWNLOAD_LIMIT
    assert resp_json["rows_gt_limit"] is False
