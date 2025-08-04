import json
import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        recipient_location_zip5="abcde",
    )
    baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        action_date="2020-10-01",
    )


@pytest.mark.django_db
def test_positive_sample_query(award_data_fixture, elasticsearch_transaction_index):
    """
    A super simple direct search against Elasticsearch that returns one record.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    elasticsearch_transaction_index.update_index()
    query = {"query": {"bool": {"must": [{"match": {"recipient_location_zip5": "abcde"}}]}}, "_source": ["award_id"]}

    client = elasticsearch_transaction_index.client
    response = client.search(index=elasticsearch_transaction_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1


@pytest.mark.django_db
def test_negative_sample_query(award_data_fixture, elasticsearch_transaction_index):
    """
    A super simple direct search against Elasticsearch that returns no results.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    elasticsearch_transaction_index.update_index()
    query = {"query": {"bool": {"must": [{"match": {"recipient_location_zip5": "edcba"}}]}}, "_source": ["award_id"]}

    client = elasticsearch_transaction_index.client
    response = client.search(index=elasticsearch_transaction_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_a_search_endpoint(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    """
    An example of how one might test a keyword search.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    query = {
        "filters": {"keyword": "IND12PB00323", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": [
            "Award ID",
            "Mod",
            "Recipient Name",
            "Action Date",
            "Transaction Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type",
        ],
        "page": 1,
        "limit": 35,
        "sort": "Transaction Amount",
        "order": "desc",
    }
    response = client.post(
        "/api/v2/search/spending_by_transaction", content_type="application/json", data=json.dumps(query)
    )
    assert response.status_code == status.HTTP_200_OK
    assert len(response.data["results"]) == 1
