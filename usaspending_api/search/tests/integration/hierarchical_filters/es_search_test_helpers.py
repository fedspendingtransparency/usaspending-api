import json
from django.conf import settings

from usaspending_api.search.elasticsearch.filters.tas import TasCodes
from usaspending_api.common.experimental_api_flags import EXPERIMENTAL_API_HEADER, ELASTICSEARCH_HEADER_VALUE


def _setup_es(client, monkeypatch, elasticsearch_award_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.AwardSearch._index_name",
        settings.ES_AWARDS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_award_index.update_index()


def _query_by_tas(client, tas):
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
