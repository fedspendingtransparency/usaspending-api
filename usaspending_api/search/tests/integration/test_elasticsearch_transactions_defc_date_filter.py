import pytest
from model_bakery import baker

from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType


@pytest.mark.django_db
def test_transactions_elasticsearch_query():
    """Test the correct Transactions Elasticsearch query is generated
    when using COVID/IIJA and non-COVID/IIJA DEFC values"""

    baker.make(
        "references.DisasterEmergencyFundCode",
        code="A",
        public_law="PUBLIC LAW FOR CODE A",
        title="TITLE FOR CODE A",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="B",
        public_law="PUBLIC LAW FOR CODE B",
        title="TITLE FOR CODE B",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        public_law="PUBLIC LAW FOR CODE Z",
        title="TITLE FOR CODE Z",
        group_name="infrastructure",
        earliest_public_law_enactment_date="2021-11-15",
    )

    def_code_filters = ["A", "B", "L", "Z"]
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    def_codes_es_query = query_with_filters.generate_elasticsearch_query(filters={"def_codes": def_code_filters})

    covid_iija_es_queries = [
        def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["bool"]["should"][0]["bool"]["must"],
        def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["bool"]["should"][1]["bool"]["must"],
    ]

    other_es_queries = def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"][1]["bool"]["should"]

    # check the COVID-19 and IIJA part of the ES query
    assert [
        {"match": {"disaster_emergency_fund_codes": "L"}},
        {"range": {"action_date": {"gte": "2020-03-06"}}},
    ] in covid_iija_es_queries

    assert [
        {"match": {"disaster_emergency_fund_codes": "Z"}},
        {"range": {"action_date": {"gte": "2021-11-15"}}},
    ] in covid_iija_es_queries

    # check the non-COVID-19/non-IIJA part of the ES query
    assert {"match": {"disaster_emergency_fund_codes": "A"}} in other_es_queries
    assert {"match": {"disaster_emergency_fund_codes": "B"}} in other_es_queries


@pytest.mark.django_db
def test_transactions_elasticsearch_query_only_covid_iija():
    """Test using only COVID and IIJA DEFC values"""

    baker.make(
        "references.DisasterEmergencyFundCode",
        code="A",
        public_law="PUBLIC LAW FOR CODE A",
        title="TITLE FOR CODE A",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="B",
        public_law="PUBLIC LAW FOR CODE B",
        title="TITLE FOR CODE B",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        public_law="PUBLIC LAW FOR CODE Z",
        title="TITLE FOR CODE Z",
        group_name="infrastructure",
        earliest_public_law_enactment_date="2021-11-15",
    )

    def_code_filters = ["L", "Z"]
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    def_codes_es_query = query_with_filters.generate_elasticsearch_query(filters={"def_codes": def_code_filters})

    covid_iija_es_queries = [
        def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"][0]["bool"]["must"],
        def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"][1]["bool"]["must"],
    ]

    assert [
        {"match": {"disaster_emergency_fund_codes": "L"}},
        {"range": {"action_date": {"gte": "2020-03-06"}}},
    ] in covid_iija_es_queries

    assert [
        {"match": {"disaster_emergency_fund_codes": "Z"}},
        {"range": {"action_date": {"gte": "2021-11-15"}}},
    ] in covid_iija_es_queries


@pytest.mark.django_db
def test_transactions_elasticsearch_query_non_covid_iija():
    """Test using only non-COVID and non-IIJA DEFC values"""

    baker.make(
        "references.DisasterEmergencyFundCode",
        code="A",
        public_law="PUBLIC LAW FOR CODE A",
        title="TITLE FOR CODE A",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="B",
        public_law="PUBLIC LAW FOR CODE B",
        title="TITLE FOR CODE B",
        group_name=None,
        earliest_public_law_enactment_date=None,
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        public_law="PUBLIC LAW FOR CODE Z",
        title="TITLE FOR CODE Z",
        group_name="infrastructure",
        earliest_public_law_enactment_date="2021-11-15",
    )

    def_code_filters = ["A", "B"]
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    def_codes_es_query = query_with_filters.generate_elasticsearch_query(filters={"def_codes": def_code_filters})

    other_es_queries = def_codes_es_query.to_dict()["bool"]["must"][0]["bool"]["should"]

    assert {"match": {"disaster_emergency_fund_codes": "A"}} in other_es_queries
    assert {"match": {"disaster_emergency_fund_codes": "B"}} in other_es_queries
