import json
import pytest


@pytest.mark.django_db
def test_the_thing(client, award):
    resp = client.post(
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
                },
            }
        ),
    )

    assert resp.json()["results"] == [{"internal_id": 1, "Award ID": "abcdefg", "generated_internal_id": "AWARD_{}"}]
