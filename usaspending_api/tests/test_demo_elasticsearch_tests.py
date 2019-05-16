import json
import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def award_data_fixture(db):
    mommy.make(
        'references.LegalEntity',
        legal_entity_id=1
    )
    mommy.make(
        'awards.TransactionNormalized',
        id=1,
        award_id=1,
        action_date='2010-10-01',
        is_fpds=True,
        type='A'
    )
    mommy.make(
        'awards.TransactionFPDS',
        transaction_id=1,
        legal_entity_zip5='abcde',
        piid='IND12PB00323'
    )
    mommy.make(
        'awards.Award',
        id=1,
        latest_transaction_id=1,
        recipient_id=1,
        is_fpds=True,
        type='A',
        piid='IND12PB00323'
    )


def test_positive_sample_query(db, award_data_fixture, elasticsearch_transaction_index):
    """
    A super simple direct search against Elasticsearch that returns one record.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    elasticsearch_transaction_index.update_index()
    query = {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "recipient_location_zip5": "abcde"
                    }
                }]
            }
        },
        "_source": ["award_id"]
    }

    client = elasticsearch_transaction_index.client
    response = client.search(
        elasticsearch_transaction_index.index_name,
        elasticsearch_transaction_index.doc_type,
        query
    )
    assert response["hits"]["total"] == 1


def test_negative_sample_query(db, award_data_fixture, elasticsearch_transaction_index):
    """
    A super simple direct search against Elasticsearch that returns no results.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    elasticsearch_transaction_index.update_index()
    query = {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "recipient_location_zip5": "edcba"
                    }
                }]
            }
        },
        "_source": ["award_id"]
    }

    client = elasticsearch_transaction_index.client
    response = client.search(
        elasticsearch_transaction_index.index_name,
        elasticsearch_transaction_index.doc_type,
        query
    )
    assert response["hits"]["total"] == 0


def test_a_search_endpoint(client, db, award_data_fixture, elasticsearch_transaction_index):
    """
    An example of how one might test a keyword search.
    """
    # This is the important part.  This ensures data is loaded into your Elasticsearch.
    elasticsearch_transaction_index.update_index()
    query = {
        "filters": {
            "keyword": "IND12PB00323",
            "award_type_codes": ["A", "B", "C", "D"]
        },
        "fields": [
            "Award ID", "Mod", "Recipient Name", "Action Date", "Transaction Amount",
            "Awarding Agency", "Awarding Sub Agency", "Award Type"
        ],
        "page": 1,
        "limit": 35,
        "sort": "Transaction Amount",
        "order": "desc"
    }
    response = client.post(
        "/api/v2/search/spending_by_transaction",
        content_type="application/json",
        data=json.dumps(query)
    )
    assert response.status_code == status.HTTP_200_OK
    assert len(response.data["results"]) == 1
