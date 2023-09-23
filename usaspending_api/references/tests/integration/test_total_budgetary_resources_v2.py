import pytest
from model_bakery import baker
from rest_framework import status
from decimal import Decimal

from usaspending_api.common.helpers.generic_helper import get_account_data_time_period_message


@pytest.fixture
def create_gtas_data():
    baker.make("references.GTASSF133Balances", id=1, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=1)
    baker.make("references.GTASSF133Balances", id=2, fiscal_year=2020, fiscal_period=2, total_budgetary_resources_cpe=2)
    baker.make("references.GTASSF133Balances", id=3, fiscal_year=2020, fiscal_period=3, total_budgetary_resources_cpe=4)
    baker.make("references.GTASSF133Balances", id=4, fiscal_year=2019, fiscal_period=2, total_budgetary_resources_cpe=8)


@pytest.mark.django_db
def test_no_params(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {"fiscal_year": 2020, "fiscal_period": 3, "total_budgetary_resources": Decimal(4)},
            {"fiscal_year": 2020, "fiscal_period": 2, "total_budgetary_resources": Decimal(3)},
            {"fiscal_year": 2019, "fiscal_period": 2, "total_budgetary_resources": Decimal(8)},
        ],
        "messages": [get_account_data_time_period_message()],
    }


@pytest.mark.django_db
def test_just_fy(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2020")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {"fiscal_year": 2020, "fiscal_period": 3, "total_budgetary_resources": Decimal(4)},
            {"fiscal_year": 2020, "fiscal_period": 2, "total_budgetary_resources": Decimal(3)},
        ],
        "messages": [],
    }


@pytest.mark.django_db
def test_fy_and_fp(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2020&fiscal_period=2")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [{"fiscal_year": 2020, "fiscal_period": 2, "total_budgetary_resources": Decimal(3)}],
        "messages": [],
    }

    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2019&fiscal_period=2")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [{"fiscal_year": 2019, "fiscal_period": 2, "total_budgetary_resources": Decimal(8)}],
        "messages": [],
    }

    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2019&fiscal_period=4")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {"results": [], "messages": []}


@pytest.mark.django_db
def test_bad_params(client, create_gtas_data):
    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_period=3")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    resp = client.get("/api/v2/references/total_budgetary_resources/?fiscal_year=2015&fiscal_period=1")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
