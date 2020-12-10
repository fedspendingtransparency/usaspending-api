import pytest
from model_mommy import mommy
from rest_framework import status
from decimal import Decimal


@pytest.fixture
def create_gtas_data():
    mommy.make("references.GTASSF133Balances", id=1, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=2)
    mommy.make("references.GTASSF133Balances", id=2, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=3)


# Test cases:
#     just FY
#     FP & FY
    # FY, bad FP
    # money summed
    # data not in results

@pytest.mark.django_db
def test_award_type_endpoint(client, create_gtas_data):

    """Test the total_budgetary_resources endpoint with bad parameters."""
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_period=3")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    """Test the total_budgetary_resources endpoint with no parameters."""
    resp = client.get("/api/v2/references/total_budgetary_resources/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "fiscal_year": 2020,
                "fiscal_period": 2,
                "total_budgetary_resources": Decimal(5),
            },
        ]
    }
