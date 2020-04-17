import json
import pytest

from usaspending_api.search.elasticsearch.filters.tas import TasCodes
from usaspending_api.common.experimental_api_flags import EXPERIMENTAL_API_HEADER, ELASTICSEARCH_HEADER_VALUE


@pytest.mark.django_db
def test_match_from_fa(client, elasticsearch_award_index, award_with_tas):
    elasticsearch_award_index.update_index()
    resp = _query_by_tas(client, {"require": [["123-2345"]]})

    assert resp.json()["results"] == [{"internal_id": 1, "Award ID": "abcdefg", "generated_internal_id": "AWARD_{}"}]


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
