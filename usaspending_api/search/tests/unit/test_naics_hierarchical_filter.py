from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.elasticsearch.naics import NaicsCodes


def test_generate_elasticsearch_query_exclude():
    """Test the generated Elasticsearch query for NAICS codes that should be excluded."""

    assert NaicsCodes.generate_elasticsearch_query({"exclude": [10, 20, 30]}, QueryType.TRANSACTIONS).to_dict() == {
        "query_string": {"query": "(NOT 10*) AND (NOT 20*) AND (NOT 30*)", "default_field": "naics_code.keyword"}
    }


def test_generate_elasticsearch_query_require():
    """Test the generated Elasticsearch query for NAICS codes that should be required."""

    assert NaicsCodes.generate_elasticsearch_query({"require": [70, 80, 90]}, QueryType.TRANSACTIONS).to_dict() == {
        "query_string": {"query": "(70*) OR (80*) OR (90*)", "default_field": "naics_code.keyword"}
    }


def test_generate_elasticsearch_query_require_exclude():
    """Test the generated Elasticsearch query for NAICS codes that should be required and excluded.

    Note: When both `require` and `exclude` are in the API request, we ignore the `exclude` value because
        only including the values in `require` means that we would be filtering out the values in `exclude`
        anyway.
    """

    assert NaicsCodes.generate_elasticsearch_query(
        {"exclude": [10, 20, 30], "require": [70, 80, 90]}, QueryType.TRANSACTIONS
    ).to_dict() == {"query_string": {"query": "(70*) OR (80*) OR (90*)", "default_field": "naics_code.keyword"}}
