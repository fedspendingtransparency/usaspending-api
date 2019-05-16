import pytest

from model_mommy import mommy


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
        is_fpds=True
    )
    mommy.make(
        'awards.TransactionFPDS',
        transaction_id=1,
        legal_entity_zip5='abcde'
    )
    mommy.make(
        'awards.Award',
        id=1,
        latest_transaction_id=1,
        recipient_id=1,
        is_fpds=True
    )


def test_sample_query(db, award_data_fixture, elasticsearch_transaction_index):
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


def test_sample_query2(db, award_data_fixture, elasticsearch_transaction_index):
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
