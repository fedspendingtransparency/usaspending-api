import pytest
from model_bakery import baker

from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch, TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.filters.elasticsearch.filter import QueryType


@pytest.fixture
def es_test_data_fixture(db):
    award_search1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="UNIQUE_AWARD_ID_1",
        recipient_name="OR CONSTRUCTION COMPANY ASSOCIATES",
        action_date="2018-01-01",
    )
    award_search2 = baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="UNIQUE_AWARD_ID_2",
        recipient_name="AND DELIVERIES FAKE COMPANY",
        action_date="2019-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        sub_awardee_or_recipient_legal="OR CONSTRUCTION COMPANY ASSOCIATES",
        award=award_search1,
        action_date="2018-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        sub_awardee_or_recipient_legal="AND DELIVERIES FAKE COMPANY",
        award=award_search2,
        action_date="2019-01-01",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award=award_search1,
        action_date="2018-01-01",
        transaction_description="Description for test_1 transaction.",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award=award_search2,
        action_date="2019-01-01",
        transaction_description="Description for test_2 transaction.",
    )


@pytest.mark.django_db
def test_es_award_seach_with_reserved_words(client, monkeypatch, elasticsearch_award_index, es_test_data_fixture):
    """Test that reserved Elasticsearch words, like "OR" and "AND", are properly escaped if they are included in a
    recipient name search.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Test the "OR" keyword
    filters = {"recipient_search_text": ["OR CONSTRUCTION"]}
    query_with_filters = QueryWithFilters(QueryType.AWARDS)
    filter_query = query_with_filters.generate_elasticsearch_query(filters)
    search = AwardSearch().filter(filter_query)
    results = search.handle_execute()

    assert (
        filter_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["query_string"]["query"] == "\\OR CONSTRUCTION*"
    )
    assert len(results["hits"]["hits"]) == 1
    assert results["hits"]["hits"][0]["_source"]["recipient_name"] == "OR CONSTRUCTION COMPANY ASSOCIATES"

    # Test the "AND" keyword
    filters = {"recipient_search_text": ["AND DELIVERIES"]}
    query_with_filters = QueryWithFilters(QueryType.AWARDS)
    filter_query = query_with_filters.generate_elasticsearch_query(filters)
    search = AwardSearch().filter(filter_query)
    results = search.handle_execute()

    assert (
        filter_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["query_string"]["query"] == "\\AND DELIVERIES*"
    )
    assert len(results["hits"]["hits"]) == 1
    assert results["hits"]["hits"][0]["_source"]["recipient_name"] == "AND DELIVERIES FAKE COMPANY"


@pytest.mark.django_db
def test_es_subaward_seach_with_reserved_words(client, monkeypatch, elasticsearch_subaward_index, es_test_data_fixture):
    """Test that reserved Elasticsearch words, like "OR" and "AND", are properly escaped if they are included in a
    recipient name search.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Test the "OR" keyword
    filters = {"recipient_search_text": ["OR CONSTRUCTION"]}
    query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
    filter_query = query_with_filters.generate_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()

    assert (
        filter_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["query_string"]["query"] == "\\OR CONSTRUCTION"
    )
    assert len(results["hits"]["hits"]) == 1
    assert (
        results["hits"]["hits"][0]["_source"]["sub_awardee_or_recipient_legal"] == "OR CONSTRUCTION COMPANY ASSOCIATES"
    )

    # Test the "AND" keyword
    filters = {"recipient_search_text": ["AND DELIVERIES"]}
    query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
    filter_query = query_with_filters.generate_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()

    assert filter_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["query_string"]["query"] == "\\AND DELIVERIES"
    assert len(results["hits"]["hits"]) == 1
    assert results["hits"]["hits"][0]["_source"]["sub_awardee_or_recipient_legal"] == "AND DELIVERIES FAKE COMPANY"


@pytest.mark.django_db
def test_es_description_filter(client, monkeypatch, elasticsearch_transaction_index, es_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    filters = {"description": "test_1"}
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    filter_query = query_with_filters.generate_elasticsearch_query(filters)
    search = TransactionSearch().filter(filter_query)
    results = search.handle_execute()

    assert filter_query.to_dict()["bool"]["must"][0]["multi_match"]["query"] == "test_1"
    assert len(results["hits"]["hits"]) == 1
    assert results["hits"]["hits"][0]["_source"]["transaction_description"] == "Description for test_1 transaction."
