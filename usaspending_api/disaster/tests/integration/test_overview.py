import pytest

from decimal import Decimal

OVERVIEW_URL = "/api/v2/disaster/overview/"


@pytest.mark.django_db
def test_basic_data_set(client, basic_gtas, basic_faba):
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"def_code": "M", "amount": Decimal("1.50")}],
        "total_budget_authority": Decimal("1.50"),
        "spending": {
            "award_obligations": Decimal("0.60"),
            "award_outlays": Decimal("0"),
            "total_obligations": Decimal("-3.50"),
            "total_outlays": Decimal("1.90"),
        },
    }
