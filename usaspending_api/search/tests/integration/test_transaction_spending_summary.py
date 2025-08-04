import json

import pytest
from rest_framework import status


@pytest.mark.skip
@pytest.mark.django_db
def test_transaction_spending_success(client):

    # test for needed filters
    resp = client.post(
        "/api/v2/search/transaction_spending_summary",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/transaction_spending_summary",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "keywords": ["test", "testing"],
                    "agencies": [{"type": "awarding", "tier": "toptier", "name": "Social Security Administration"}],
                    "award_amounts": [{"lower_bound": 1500000.00, "upper_bound": 1600000.00}],
                }
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
