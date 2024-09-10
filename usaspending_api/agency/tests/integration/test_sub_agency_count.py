import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/agency/{toptier_code}/sub_agency/count/{filter}"


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = {
        "toptier_code": "001",
        "fiscal_year": 2021,
        "sub_agency_count": 1,
        "office_count": 2,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_year(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))

    expected_results = {
        "toptier_code": "001",
        "fiscal_year": 2020,
        "sub_agency_count": 1,
        "office_count": 1,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2021"))

    expected_results = {
        "toptier_code": "002",
        "fiscal_year": 2021,
        "sub_agency_count": 1,
        "office_count": 1,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_award_types(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&award_type_codes=[A]"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "toptier_code": "001",
        "fiscal_year": 2021,
        "sub_agency_count": 1,
        "office_count": 1,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_agency_types(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&agency_type=funding"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "toptier_code": "001",
        "fiscal_year": 2021,
        "sub_agency_count": 1,
        "office_count": 1,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, sub_agency_data_1):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
