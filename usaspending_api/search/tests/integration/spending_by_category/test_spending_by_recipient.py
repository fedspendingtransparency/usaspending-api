import json
import logging

from elasticsearch_dsl import A
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _expected_messages():
    expected_messages = [get_time_period_message()]
    expected_messages.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. "
    )
    return expected_messages


def _make_fpds_transaction(
    id, award_id, obligation, action_date, recipient_duns, recipient_name, recipient_hash, piid=None
):
    # Transaction Search
    baker.make(
        "search.TransactionSearch",
        transaction_id=id,
        is_fpds=True,
        award_id=award_id,
        federal_action_obligation=obligation,
        generated_pragmatic_obligation=obligation,
        action_date=action_date,
        recipient_unique_id=recipient_duns,
        recipient_name=recipient_name,
        recipient_hash=recipient_hash,
        piid=piid,
    )


def test_top_1_fails_with_es_transactions_routed_dangerously(client, monkeypatch, elasticsearch_transaction_index, db):
    """
    This confirms vulnerability of high-cardinality aggregations documented in DEV-4685, that leads to inaccurate
    summing and ordering of sums when taking less buckets than the term cardinality.

    This is shown by manually applying a routing key (using a key value stuck in ``awards.piid`` field here as the
    routing key value) on index do that documents are distributed as below

    NOTE: This requires an ES cluster with at least 3 shards for the transaction index. Ours should be defaulted to 5.

    Recipient shard0   shard1   shard2   shard3   shard4
    Biz 1      $2.00
    Biz 1                       $ 7.00
    Biz 1                       $ 3.00
    Biz 1              $ 2.00
    Biz 1              $ 3.00
    Biz 1              $ 5.00
    Biz 2              $ 6.00
    Biz 2              $ 3.00
    Biz 2                       $ 2.00
    Biz 2                       $ 3.00
    Biz 2                       $ 4.00
    Biz 2     $13.00

    **IF THIS TEST FAILS**
        - Did our cluster structure change to not be 5 shards per the transaction index?
        - Did the transaction<->award DB linkage change?
        - Did we change ES version or config?
            - Investigate if Elasticsearch has changed the way they do routing or hash routing key values
    """

    # Setup data for this test
    recipient1 = "c870f759-d7bb-c074-784a-ab5867b1da3f"
    recipient2 = "0aa0ab7e-efa0-e3f4-6f55-6b0ecaf636e6"

    # Recipient Lookup
    baker.make("recipient.RecipientLookup", id=1, recipient_hash=recipient1, legal_business_name="Biz 1", duns="111")
    baker.make("recipient.RecipientLookup", id=2, recipient_hash=recipient2, legal_business_name="Biz 2", duns="222")

    # Awards
    # Jam a routing key value into the piid field, and use the derived piid value for routing documents to shards later
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=12, piid="shard_zero")
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=6, piid="shard_one")
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=9, piid="shard_two")

    # Transaction FPDS
    _make_fpds_transaction(1, 1, 2.00, "2020-01-01", "111", "Biz 1", recipient1, "shard_zero")
    _make_fpds_transaction(2, 3, 7.00, "2020-02-02", "111", "Biz 1", recipient1, "shard_two")
    _make_fpds_transaction(3, 3, 3.00, "2020-03-03", "111", "Biz 1", recipient1, "shard_two")
    _make_fpds_transaction(4, 2, 2.00, "2020-01-02", "111", "Biz 1", recipient1, "shard_one")
    _make_fpds_transaction(5, 2, 3.00, "2020-02-03", "111", "Biz 1", recipient1, "shard_one")
    _make_fpds_transaction(6, 2, 5.00, "2020-03-04", "111", "Biz 1", recipient1, "shard_one")
    _make_fpds_transaction(7, 2, 6.00, "2020-01-03", "222", "Biz 2", recipient2, "shard_one")
    _make_fpds_transaction(8, 2, 3.00, "2020-02-04", "222", "Biz 2", recipient2, "shard_one")
    _make_fpds_transaction(9, 3, 2.00, "2020-03-05", "222", "Biz 2", recipient2, "shard_two")
    _make_fpds_transaction(10, 3, 3.00, "2020-01-04", "222", "Biz 2", recipient2, "shard_two")
    _make_fpds_transaction(11, 3, 4.00, "2020-02-05", "222", "Biz 2", recipient2, "shard_two")
    _make_fpds_transaction(12, 1, 13.00, "2020-03-06", "222", "Biz 2", recipient2, "shard_zero")

    # Push DB data into the test ES cluster
    # NOTE: Force routing of documents by the piid field, which will separate them int 3 groups, leading to an
    # inaccurate sum and ordering of sums

    # Using piid (derived from the transaction's award) to route transaction documents to shards
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, routing="piid")

    search = TransactionSearch()
    total = search.handle_count()
    assert total == 12, "Should have seen 12 documents indexed for this test"

    group_by_agg = A("terms", field="recipient_hash", size=1, shard_size=1, order={"sum_agg": "desc"})
    sum_agg = A("sum", field="generated_pragmatic_obligation")
    search.aggs.bucket("results", group_by_agg).metric("sum_agg", sum_agg)

    logging.getLogger("console").debug(f"=>->=>->=>-> WILL RUN THIS ES QUERY: \n {search.extra(size=0).to_dict()}")
    response = search.extra(size=0).handle_execute().to_dict()
    results = []
    for bucket in response["aggregations"]["results"]["buckets"]:
        results.append({"key": bucket["key"], "sum": bucket["sum_agg"]["value"]})

    assert len(results) == 1
    assert results[0]["key"] == str(
        recipient1
    ), "This botched 'Top 1' sum agg should have incorrectly chosen the lesser recipient"
    assert results[0]["sum"] == 20.0, "The botched 'Top 1' sum agg should have incorrectly summed up recipient totals"


def test_top_1_with_es_transactions_routed_by_recipient(client, monkeypatch, elasticsearch_transaction_index, db):
    """
    This tests the approach to compensating for high-cardinality aggregations
    documented in DEV-4685, to ensure accuracy and completeness of aggregations
    and sorting even when taking less buckets than the term cardinality.

    Without the code to route indexing of transaction documents in elasticsearch
    to shards by the `recipient_agg_key`, which was added to
    :meth:`usaspending_api.etl.elasticsearch_loader_helpers.csv_chunk_gen`,
    the below agg queries should lead to inaccurate results, as shown in the DEV-4538.

    With routing by recipient, documents will be allocated to shards as below

    Recipient shard0   shard1   shard2   shard3   shard4
    Biz 1      $2.00
    Biz 1      $ 7.00
    Biz 1      $ 3.00
    Biz 1      $ 2.00
    Biz 1      $ 3.00
    Biz 1      $ 5.00
    Biz 2              $ 6.00
    Biz 2              $ 3.00
    Biz 2              $ 2.00
    Biz 2              $ 3.00
    Biz 2              $ 4.00
    Biz 2              $13.00

    **IF THIS TEST FAILS**
        - Are we still using the TestElasticSearchIndex fixture to help with pushing test data to ES?
        - Did TestElasticSearchIndex indexing / routing behavior change?
        - Did our cluster structure change to not be 5 shards per the transaction index?
        - Did the transaction<->recipient DB linkage change?
        - Did we change ES version or config?
            - Investigate if Elasticsearch has changed the way they do routing or hash routing key values
    """

    # Setup data for this test

    recipient1 = "c870f759-d7bb-c074-784a-ab5867b1da3f"
    recipient2 = "0aa0ab7e-efa0-e3f4-6f55-6b0ecaf636e6"

    # Recipient Lookup
    baker.make("recipient.RecipientLookup", id=1, recipient_hash=recipient1, legal_business_name="Biz 1", duns="111")
    baker.make("recipient.RecipientLookup", id=2, recipient_hash=recipient2, legal_business_name="Biz 2", duns="222")

    # Transaction FPDS
    _make_fpds_transaction(1, 1, 2.00, "2020-01-01", "111", "Biz 1", recipient1)
    _make_fpds_transaction(2, 3, 7.00, "2020-02-02", "111", "Biz 1", recipient1)
    _make_fpds_transaction(3, 3, 3.00, "2020-03-03", "111", "Biz 1", recipient1)
    _make_fpds_transaction(4, 2, 2.00, "2020-01-02", "111", "Biz 1", recipient1)
    _make_fpds_transaction(5, 2, 3.00, "2020-02-03", "111", "Biz 1", recipient1)
    _make_fpds_transaction(6, 2, 5.00, "2020-03-04", "111", "Biz 1", recipient1)
    _make_fpds_transaction(7, 2, 6.00, "2020-01-03", "222", "Biz 2", recipient2)
    _make_fpds_transaction(8, 2, 3.00, "2020-02-04", "222", "Biz 2", recipient2)
    _make_fpds_transaction(9, 3, 2.00, "2020-03-05", "222", "Biz 2", recipient2)
    _make_fpds_transaction(10, 3, 3.00, "2020-01-04", "222", "Biz 2", recipient2)
    _make_fpds_transaction(11, 3, 4.00, "2020-02-05", "222", "Biz 2", recipient2)
    _make_fpds_transaction(12, 1, 13.00, "2020-03-06", "222", "Biz 2", recipient2)

    # Awards
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=12)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=6)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=9)

    # Push DB data into the test ES cluster
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    search = TransactionSearch()
    total = search.handle_count()
    assert total == 12, "Should have seen 12 documents indexed for this test"

    group_by_agg = A("terms", field="recipient_hash", size=1, shard_size=1, order={"sum_agg": "desc"})
    sum_agg = A("sum", field="generated_pragmatic_obligation")
    search.aggs.bucket("results", group_by_agg).metric("sum_agg", sum_agg)

    logging.getLogger("console").debug(f"=>->=>->=>-> WILL RUN THIS ES QUERY: \n {search.extra(size=0).to_dict()}")
    response = search.extra(size=0).handle_execute().to_dict()
    results = []
    for bucket in response["aggregations"]["results"]["buckets"]:
        results.append({"key": bucket["key"], "sum": bucket["sum_agg"]["value"]})
    assert len(results) == 1
    assert results[0]["key"] == str(
        recipient2
    ), "The 'Top 1' sum agg incorrectly chose the recipient with a lesser total sum"
    assert results[0]["sum"] == 31.0, "The 'Top 1' sum agg incorrectly summed up recipient totals"


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/recipient_duns",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/recipient_duns",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "recipient_duns",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 5000000.0,
                "code": None,
                "name": "MULTIPLE RECIPIENTS",
                "recipient_id": None,
                "uei": None,
                "total_outlays": None,
            },
            {
                "amount": 5000000.0,
                "code": None,
                "name": "MULTIPLE RECIPIENTS",
                "recipient_id": "64af1cb7-993c-b64b-1c58-f5289af014c0-R",
                "uei": None,
                "total_outlays": None,
            },
            {
                "amount": 550000.0,
                "code": "123456789",
                "name": None,
                "recipient_id": "f1400310-181e-9a06-ac94-0d80a819bb5e-R",
                "uei": "123456789AAA",
                "total_outlays": None,
            },
            {
                "amount": 5000.0,
                "code": "096354360",
                "name": "MULTIPLE RECIPIENTS",
                "recipient_id": "b1bcf17e-d0dc-d9ad-866c-ca262cb05029-R",
                "uei": "096354360AAA",
                "total_outlays": None,
            },
            {
                "amount": 500.0,
                "code": "987654321",
                "name": "RECIPIENT 3",
                "recipient_id": "3523fd0b-c1f0-ddac-e217-7b7b25fad06f-C",
                "uei": "987654321AAA",
                "total_outlays": None,
            },
            {
                "amount": 50.0,
                "code": "456789123",
                "name": "RECIPIENT 2",
                "recipient_id": "7976667a-dd95-2b65-5f4e-e340c686a346-R",
                "uei": "UEIAAABBBCCC",
                "total_outlays": None,
            },
            {"amount": 30.0, "code": None, "name": None, "recipient_id": None, "uei": None, "total_outlays": None},
            {
                "amount": 5.0,
                "code": None,
                "name": "RECIPIENT 1",
                "recipient_id": "5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R",
                "uei": None,
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/recipient_duns",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2007-10-01", "end_date": "2008-09-30"}]}}),
    )
    expected_response = {
        "category": "recipient_duns",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_recipient_search_text_uei(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/recipient_duns",
        content_type="application/json",
        data=json.dumps({"filters": {"recipient_search_text": ["UEIAAABBBCCC"]}}),
    )
    expected_response = {
        "category": "recipient_duns",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 50.0,
                "code": "456789123",
                "name": "RECIPIENT 2",
                "recipient_id": "7976667a-dd95-2b65-5f4e-e340c686a346-R",
                "uei": "UEIAAABBBCCC",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_recipient_search_text_duns(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/recipient_duns",
        content_type="application/json",
        data=json.dumps({"filters": {"recipient_search_text": ["987654321"]}}),
    )
    expected_response = {
        "category": "recipient_duns",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 500.0,
                "code": "987654321",
                "name": "RECIPIENT 3",
                "recipient_id": "3523fd0b-c1f0-ddac-e217-7b7b25fad06f-C",
                "uei": "987654321AAA",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
