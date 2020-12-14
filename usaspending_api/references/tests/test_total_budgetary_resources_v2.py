import pytest
from model_mommy import mommy
from rest_framework import status
from decimal import Decimal


@pytest.fixture
def create_gtas_data():
    mommy.make("references.GTASSF133Balances", id=1, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=1)
    mommy.make("references.GTASSF133Balances", id=2, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=2)
    mommy.make("references.GTASSF133Balances", id=3, fiscal_year=2020, fiscal_period=3, total_budgetary_resources_cpe=4)
    mommy.make("references.GTASSF133Balances", id=4, fiscal_year=2019, fiscal_period=2, total_budgetary_resources_cpe=8)

@pytest.mark.django_db
def test_no_params(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "fiscal_year": 2019,
                "fiscal_period": 2,
                "total_budgetary_resources": Decimal(8),
            },
            {
                "fiscal_year": 2020,
                "fiscal_period": 2,
                "total_budgetary_resources": Decimal(3),
            },
            {
                "fiscal_year": 2020,
                "fiscal_period": 3,
                "total_budgetary_resources": Decimal(4),
            },
        ]
    }

@pytest.mark.django_db
def test_just_fy(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2020")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "fiscal_year": 2020,
                "fiscal_period": 2,
                "total_budgetary_resources": Decimal(3),
            },
            {
                "fiscal_year": 2020,
                "fiscal_period": 3,
                "total_budgetary_resources": Decimal(4),
            },
        ]
    }

@pytest.mark.django_db
def test_fy_and_fp(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2020&fiscal_period=2")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "fiscal_year": 2020,
                "fiscal_period": 2,
                "total_budgetary_resources": Decimal(3),
            },
        ]
    }

@pytest.mark.django_db
def test_bad_params(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_period=3")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2015&fiscal_period=0")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
