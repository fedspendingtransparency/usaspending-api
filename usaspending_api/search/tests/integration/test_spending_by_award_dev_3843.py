import json
import pytest

from rest_framework import status
from usaspending_api.awards.models import Award, Subaward
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_id_search_data(spending_by_award_test_data):
    """
    Take the existing spending by award test data and tweak it for our test.  We need a couple
    of award ids (piid or fain or uri) where only spacing differs.
    """
    Award.objects.filter(id=1).update(piid="abc111")
    Award.objects.filter(id=2).update(piid="abc 111")
    Award.objects.filter(id=3).update(piid="abc       111")

    Subaward.objects.filter(id=1).update(piid="abc111")
    Subaward.objects.filter(id=2).update(piid="abc111")
    Subaward.objects.filter(id=3).update(piid="abc 111")
    Subaward.objects.filter(id=6).update(piid="abc       111")


def build_request_data(award_ids, subawards):
    return json.dumps(
        {
            "filters": {
                "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                "award_type_codes": ["A", "B", "C", "D"],
                "award_ids": award_ids,
            },
            "fields": ["Sub-Award ID" if subawards else "Award ID"],
            "sort": "Sub-Award ID" if subawards else "Award ID",
            "subawards": subawards,
        }
    )


@pytest.mark.django_db
def test_award_id_search(client, monkeypatch, elasticsearch_award_index, award_id_search_data):
    """
    DEV-3843 requested that we support searching exact award id matches when awards ids are surrounded by quotes.
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Control test.  This should return only the one award.
    resp = client.post(
        "/api/v2/search/spending_by_award", content_type="application/json", data=build_request_data(["abc111"], False)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["Award ID"] == "abc111"

    # The behavior pre DEV-3843.  Should return both variants of "abc 111" (with spacing).
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=build_request_data(["abc       111"], False),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2
    assert {resp.data["results"][0]["Award ID"], resp.data["results"][1]["Award ID"]} == {"abc 111", "abc       111"}

    # The new DEV-3843 behavior.  Should return only the exact match.
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=build_request_data(['"abc       111"'], False),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["Award ID"] == "abc       111"

    # Just for giggles, two exact matches.
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=build_request_data(['"abc       111"', "abc111"], False),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2
    assert {resp.data["results"][0]["Award ID"], resp.data["results"][1]["Award ID"]} == {"abc111", "abc       111"}

    # And finally, everything by exact match.
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=build_request_data(['"abc       111"', '"abc 111"', '"abc111"'], False),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 3
    assert {
        resp.data["results"][0]["Award ID"],
        resp.data["results"][1]["Award ID"],
        resp.data["results"][2]["Award ID"],
    } == {"abc111", "abc 111", "abc       111"}

    # Subaward check.
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=build_request_data(['"abc       111"'], True),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["Sub-Award ID"] == "66666"
