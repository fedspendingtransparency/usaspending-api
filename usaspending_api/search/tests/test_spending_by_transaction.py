import json
import pytest

from time import perf_counter
from rest_framework import status


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_transaction_kws_success(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_by_transaction/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
                "fields": ["Award ID", "Recipient Name", "Mod"],
                "page": 1,
                "limit": 5,
                "sort": "Award ID",
                "order": "desc",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_transaction_kws_failure(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_by_transaction/", content_type="application/json", data=json.dumps({"filters": {}})
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_no_intersection(client, refresh_matviews):
    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D", "no intersection"]},
        "fields": ["Award ID", "Recipient Name", "Mod"],
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }
    api_start = perf_counter()

    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(request))
    api_end = perf_counter()
    assert resp.status_code == status.HTTP_200_OK
    assert api_end - api_start < 0.5, "Response took over 0.5s! Investigate why"
    assert len(resp.data["results"]) == 0, "Results returned, there should be 0"
