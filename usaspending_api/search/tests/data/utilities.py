from builtins import Exception

from django.conf import settings


def setup_elasticsearch_test(monkeypatch, index_fixture, **options):
    if index_fixture.index_type == "award":
        search_wrapper = "AwardSearch"
        query_alias = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "covid19-faba":
        search_wrapper = "AccountSearch"
        query_alias = settings.ES_COVID19_FABA_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "transaction":
        search_wrapper = "TransactionSearch"
        query_alias = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX
    else:
        raise Exception("Invalid index type")

    monkeypatch.setattr(
        f"usaspending_api.common.elasticsearch.search_wrappers.{search_wrapper}._index_name", query_alias
    )
    index_fixture.update_index(**options)
