from builtins import Exception

from django.conf import settings


def setup_elasticsearch_test(monkeypatch, index_fixture, **options):
    if index_fixture.index_type == "award":
        search_wrapper = "AwardSearch"
        query_alias = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "subaward":
        search_wrapper = "SubawardSearch"
        query_alias = settings.ES_SUBAWARD_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "transaction":
        search_wrapper = "TransactionSearch"
        query_alias = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "recipient":
        search_wrapper = "RecipientSearch"
        query_alias = settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX
    elif index_fixture.index_type == "location":
        search_wrapper = "TransactionSearch"
        query_alias = settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX
    else:
        raise Exception("Invalid index type")

    monkeypatch.setattr(
        f"usaspending_api.common.elasticsearch.search_wrappers.{search_wrapper}._index_name", query_alias
    )
    index_fixture.update_index(**options)
