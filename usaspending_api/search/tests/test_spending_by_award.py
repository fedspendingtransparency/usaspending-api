import json
import pytest

from django.db import connection
from model_mommy import mommy
from rest_framework import status
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {"subawards": True, "fields": ["Sub-Award ID"], "sort": "Sub-Award ID", "filters": all_filters()}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_no_intersection(client):

    mommy.make("references.LegalEntity", legal_entity_id=1)
    mommy.make("awards.Award", id=1, type="A", recipient_id=1, latest_transaction_id=1)
    mommy.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True)
    mommy.make("awards.TransactionFPDS", transaction_id=1)

    with connection.cursor() as cursor:
        cursor.execute("refresh materialized view concurrently mv_contract_award_search")

    request = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {"award_type_codes": ["A", "B", "C", "D"]},
    }

    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0, "Results returned, there should be 0"
