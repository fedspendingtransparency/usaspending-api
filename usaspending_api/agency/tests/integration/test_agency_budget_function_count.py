import pytest

from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/{code}/budget_function/count/{filter}"


@pytest.mark.django_db
def test_budget_function_count_success(client, monkeypatch, agency_account_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    resp = client.get(url.format(code="007", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["budget_function_count"] == 3
    assert resp.data["budget_sub_function_count"] == 3

    resp = client.get(url.format(code="007", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["budget_function_count"] == 0
    assert resp.data["budget_sub_function_count"] == 0


@pytest.mark.django_db
def test_budget_function_count_too_early(client, agency_account_data):
    resp = client.get(url.format(code="007", filter="?fiscal_year=2007"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_budget_function_count_future(client, agency_account_data):
    resp = client.get(url.format(code="007", filter=f"?fiscal_year={current_fiscal_year() + 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_budget_function_count_specific(client, agency_account_data):
    resp = client.get(url.format(code="008", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["budget_function_count"] == 1
    assert resp.data["budget_sub_function_count"] == 1

    resp = client.get(url.format(code="008", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["budget_function_count"] == 1
    assert resp.data["budget_sub_function_count"] == 1


@pytest.mark.django_db
def test_budget_function_count_ignore_duplicates(client, agency_account_data):
    resp = client.get(url.format(code="009", filter="?fiscal_year=2019"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["budget_function_count"] == 1
    assert resp.data["budget_sub_function_count"] == 1
