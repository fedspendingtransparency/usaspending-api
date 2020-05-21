import json

from usaspending_api.search.filters.elasticsearch.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _setup_es(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)


def query_by_tas(client, tas):
    return client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": False,
                "fields": ["Award ID"],
                "sort": "Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    TasCodes.underscore_name: tas,
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )


def query_by_tas_subaward(client, tas):
    return client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": True,
                "fields": ["Sub-Award ID"],
                "sort": "Sub-Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    TasCodes.underscore_name: tas,
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )


def query_by_treasury_account_components(client, tas, treasury_accounts):
    filters = {}
    if tas:
        filters[TasCodes.underscore_name] = tas

    if treasury_accounts:
        filters[TreasuryAccounts.underscore_name] = treasury_accounts

    return client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": False,
                "fields": ["Award ID"],
                "sort": "Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                    **filters,
                },
            }
        ),
    )


def query_by_treasury_account_components_subaward(client, tas, treasury_accounts):
    filters = {}
    if tas:
        filters[TasCodes.underscore_name] = tas

    if treasury_accounts:
        filters[TreasuryAccounts.underscore_name] = treasury_accounts

    return client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": True,
                "fields": ["Sub-Award ID"],
                "sort": "Sub-Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                    **filters,
                },
            }
        ),
    )
