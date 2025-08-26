import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_transaction_grouped/"


@pytest.fixture
def transaction_data():
    baker.make(
        "search.AwardSearch",
        description="award 1",
        award_id=1,
        display_award_id="IND12PB00323",
        generated_unique_award_id="IND12PB00323-generated",
        latest_transaction_id=2,
        is_fpds=True,
        type="A",
        action_date="2021-01-01",
        transaction_count=2,
        generated_pragmatic_obligation=135.00,
    )
    baker.make(
        "search.AwardSearch",
        description="award 2",
        award_id=2,
        display_award_id="BOI1243L98AS",
        generated_unique_award_id="BOI1243L98AS-generated",
        latest_transaction_id=4,
        is_fpds=True,
        type="A",
        action_date="2020-01-01",
        transaction_count=2,
        generated_pragmatic_obligation=65.00,
    )


@pytest.mark.django_db
def test_spending_by_transaction_grouped_success(client, monkeypatch, transaction_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1"], "award_type_codes": ["A"], "award_ids": ["IND12PB00323"]},
                "sort": "transaction_obligation",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 1
    assert resp_results[0]["award_id"] == "IND12PB00323"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 135.00
    assert resp_results[0]["award_generated_internal_id"] == "IND12PB00323-generated"

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1"], "award_type_codes": ["B"]},
                "sort": "transaction_obligation",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 0

    # Test that `keyword` can be used
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keyword": "award 1", "award_type_codes": ["A"], "award_ids": ["IND12PB00323"]},
                "sort": "transaction_obligation",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 1
    assert resp_results[0]["award_id"] == "IND12PB00323"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 135.00
    assert resp_results[0]["award_generated_internal_id"] == "IND12PB00323-generated"

    # Test multiple prime awards in results
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1", "award 2"], "award_type_codes": ["A"]},
                "sort": "transaction_obligation",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 2
    assert resp_results[0]["award_id"] == "IND12PB00323"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 135.00
    assert resp_results[0]["award_generated_internal_id"] == "IND12PB00323-generated"
    assert resp_results[1]["award_id"] == "BOI1243L98AS"
    assert resp_results[1]["transaction_count"] == 2
    assert resp_results[1]["transaction_obligation"] == 65.00
    assert resp_results[1]["award_generated_internal_id"] == "BOI1243L98AS-generated"


@pytest.mark.django_db
def test_spending_by_transaction_grouped_sorting(client, monkeypatch, transaction_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Test sort order
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1", "award 2"], "award_type_codes": ["A"]},
                "sort": "award_id",
                "order": "asc",
            }
        ),
    )
    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 2
    assert resp_results[0]["award_id"] == "BOI1243L98AS"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 65.00
    assert resp_results[0]["award_generated_internal_id"] == "BOI1243L98AS-generated"
    assert resp_results[1]["award_id"] == "IND12PB00323"
    assert resp_results[1]["transaction_count"] == 2
    assert resp_results[1]["transaction_obligation"] == 135.00
    assert resp_results[1]["award_generated_internal_id"] == "IND12PB00323-generated"

    # Test sort field
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1", "award 2"], "award_type_codes": ["A"]},
                "sort": "transaction_obligation",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 2
    assert resp_results[0]["award_id"] == "IND12PB00323"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 135.00
    assert resp_results[0]["award_generated_internal_id"] == "IND12PB00323-generated"
    assert resp_results[1]["award_id"] == "BOI1243L98AS"
    assert resp_results[1]["transaction_count"] == 2
    assert resp_results[1]["transaction_obligation"] == 65.00
    assert resp_results[1]["award_generated_internal_id"] == "BOI1243L98AS-generated"

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keywords": ["award 1", "award 2"], "award_type_codes": ["A"]},
                "sort": "award_generated_internal_id",
                "order": "asc",
            }
        ),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 2
    assert resp_results[0]["award_id"] == "BOI1243L98AS"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 65.00
    assert resp_results[0]["award_generated_internal_id"] == "BOI1243L98AS-generated"
    assert resp_results[1]["award_id"] == "IND12PB00323"
    assert resp_results[1]["transaction_count"] == 2
    assert resp_results[1]["transaction_obligation"] == 135.00
    assert resp_results[1]["award_generated_internal_id"] == "IND12PB00323-generated"


@pytest.mark.django_db
def test_spending_by_transaction_grouped_pagination(client, monkeypatch, transaction_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {"filters": {"award_type_codes": ["A"]}, "sort": "transaction_obligation", "page": 1, "limit": 1}
        ),
    )

    resp_results = resp.data.get("results", {})
    page_metadata = resp.data.get("page_metadata", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 1
    assert page_metadata["page"] == 1
    assert page_metadata["hasNext"] is True
    assert page_metadata["hasPrevious"] is False
    assert resp_results[0]["award_id"] == "IND12PB00323"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 135.00
    assert resp_results[0]["award_generated_internal_id"] == "IND12PB00323-generated"

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {"filters": {"award_type_codes": ["A"]}, "sort": "transaction_obligation", "page": 2, "limit": 1}
        ),
    )

    resp_results = resp.data.get("results", {})
    page_metadata = resp.data.get("page_metadata", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) == 1
    assert page_metadata["page"] == 2
    assert page_metadata["hasNext"] is False
    assert page_metadata["hasPrevious"] is True

    assert resp_results[0]["award_id"] == "BOI1243L98AS"
    assert resp_results[0]["transaction_count"] == 2
    assert resp_results[0]["transaction_obligation"] == 65.00
    assert resp_results[0]["award_generated_internal_id"] == "BOI1243L98AS-generated"
