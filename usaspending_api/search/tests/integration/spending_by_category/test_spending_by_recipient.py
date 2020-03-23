import logging
import uuid

from model_mommy import mommy
from elasticsearch_dsl import A

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.search.tests.integration.spending_by_category.utilities import setup_elasticsearch_test


def _make_fpds_transaction(id, award_id, obligation, action_date, recipient_duns, recipient_name):
    # Transaction Normalized
    mommy.make(
        "awards.TransactionNormalized",
        id=id,
        is_fpds=True,
        award_id=award_id,
        federal_action_obligation=obligation,
        action_date=action_date,
    )

    # Transaction FPDS
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=id,
        awardee_or_recipient_uniqu=recipient_duns,
        awardee_or_recipient_legal=recipient_name,
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

    """

    # Setup data for this test
    recipient1 = uuid.uuid4()
    recipient2 = uuid.uuid4()

    # Recipient Lookup
    mommy.make("recipient.RecipientLookup", id=1, recipient_hash=recipient1, legal_business_name="Biz 1", duns="111")
    mommy.make("recipient.RecipientLookup", id=2, recipient_hash=recipient2, legal_business_name="Biz 2", duns="222")

    # Transaction FPDS
    _make_fpds_transaction(1, 1, 2.00, "2020-01-01", "111", "Biz 1")
    _make_fpds_transaction(2, 3, 7.00, "2020-02-02", "111", "Biz 1")
    _make_fpds_transaction(3, 3, 3.00, "2020-03-03", "111", "Biz 1")
    _make_fpds_transaction(4, 2, 2.00, "2020-01-02", "111", "Biz 1")
    _make_fpds_transaction(5, 2, 3.00, "2020-02-03", "111", "Biz 1")
    _make_fpds_transaction(6, 2, 5.00, "2020-03-04", "111", "Biz 1")
    _make_fpds_transaction(7, 2, 6.00, "2020-01-03", "222", "Biz 2")
    _make_fpds_transaction(8, 2, 3.00, "2020-02-04", "222", "Biz 2")
    _make_fpds_transaction(9, 3, 2.00, "2020-03-05", "222", "Biz 2")
    _make_fpds_transaction(10, 3, 3.00, "2020-01-04", "222", "Biz 2")
    _make_fpds_transaction(11, 3, 4.00, "2020-02-05", "222", "Biz 2")
    _make_fpds_transaction(12, 1, 13.00, "2020-03-06", "222", "Biz 2")

    # Awards
    # Jam a routing key value into the piid field, and use the derived piid value for routing documents to shards later
    mommy.make("awards.Award", id=1, latest_transaction_id=12, piid="shard_zero")
    mommy.make("awards.Award", id=2, latest_transaction_id=6, piid="shard_one")
    mommy.make("awards.Award", id=3, latest_transaction_id=9, piid="shard_two")

    # Push DB data into the test ES cluster
    # NOTE: Force routing of documents by the piid field, which will separate them int 3 groups, leading to an
    # inaccurate sum and ordering of sums
    logging_statements = []

    # Using piid (derived from the transaction's award) to route transaction documents to shards
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements, routing="piid")

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
    print(results)
    assert len(results) == 1
    assert results[0]["key"] == str(
        recipient1
    ), "This botched 'Top 1' sum agg should have incorrectly chosen the lesser recipient"
    assert results[0]["sum"] == 20.0, "The botched 'Top 1' sum agg should have incorrectly summed up recipient totals"


def test_top_1_with_es_transactions_routed_by_recipient(client, monkeypatch, elasticsearch_transaction_index, db):
    """
    This tests the approach to compensating for high-cardinality aggregations documented in DEV-4685,
    to ensure accuracy and completeness of aggregations and sorting even when taking less buckets than the term
    cardinality.

    Without the code to route indexing of transaction documents in elasticsearch to shards by the `recipient_hash`,
    which was added to :meth:`usaspending_api.etl.es_etl_helpers.csv_chunk_gen`, the below agg queries should lead to
    inaccurate results, as shown in the DEV-4538.

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

    """

    # Setup data for this test

    recipient1 = uuid.uuid4()
    recipient2 = uuid.uuid4()

    # Recipient Lookup
    mommy.make("recipient.RecipientLookup", id=1, recipient_hash=recipient1, legal_business_name="Biz 1", duns="111")
    mommy.make("recipient.RecipientLookup", id=2, recipient_hash=recipient2, legal_business_name="Biz 2", duns="222")

    # Transaction FPDS
    _make_fpds_transaction(1, 1, 2.00, "2020-01-01", "111", "Biz 1")
    _make_fpds_transaction(2, 3, 7.00, "2020-02-02", "111", "Biz 1")
    _make_fpds_transaction(3, 3, 3.00, "2020-03-03", "111", "Biz 1")
    _make_fpds_transaction(4, 2, 2.00, "2020-01-02", "111", "Biz 1")
    _make_fpds_transaction(5, 2, 3.00, "2020-02-03", "111", "Biz 1")
    _make_fpds_transaction(6, 2, 5.00, "2020-03-04", "111", "Biz 1")
    _make_fpds_transaction(7, 2, 6.00, "2020-01-03", "222", "Biz 2")
    _make_fpds_transaction(8, 2, 3.00, "2020-02-04", "222", "Biz 2")
    _make_fpds_transaction(9, 3, 2.00, "2020-03-05", "222", "Biz 2")
    _make_fpds_transaction(10, 3, 3.00, "2020-01-04", "222", "Biz 2")
    _make_fpds_transaction(11, 3, 4.00, "2020-02-05", "222", "Biz 2")
    _make_fpds_transaction(12, 1, 13.00, "2020-03-06", "222", "Biz 2")

    # Awards
    mommy.make("awards.Award", id=1, latest_transaction_id=12)
    mommy.make("awards.Award", id=2, latest_transaction_id=6)
    mommy.make("awards.Award", id=3, latest_transaction_id=9)

    # Push DB data into the test ES cluster
    logging_statements = []
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

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
