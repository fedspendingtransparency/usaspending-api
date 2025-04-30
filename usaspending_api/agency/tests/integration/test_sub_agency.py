import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/agency/{toptier_code}/sub_agency/{filter}"


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = [
        {
            "name": "Sub-Agency 1",
            "abbreviation": "A1",
            "total_obligations": 515,
            "transaction_count": 5,
            "new_award_count": 4,
            "children": [
                {
                    "name": "Office 2",
                    "code": "0002",
                    "total_obligations": 312.0,
                    "transaction_count": 3,
                    "new_award_count": 3,
                },
                {
                    "name": "Office 1",
                    "code": "0001",
                    "total_obligations": 203.0,
                    "transaction_count": 2,
                    "new_award_count": 1,
                },
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_year(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Sub-Agency 1",
            "abbreviation": "A1",
            "total_obligations": 300.0,
            "transaction_count": 1,
            "new_award_count": 1,
            "children": [
                {
                    "name": "Office 1",
                    "code": "0001",
                    "total_obligations": 300.0,
                    "transaction_count": 1,
                    "new_award_count": 1,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Sub-Agency 2",
            "abbreviation": "A2",
            "total_obligations": 400.0,
            "transaction_count": 1,
            "new_award_count": 0,
            "children": [
                {
                    "name": "Office 2",
                    "code": "0002",
                    "total_obligations": 400.0,
                    "transaction_count": 1,
                    "new_award_count": 0,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_award_types(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&award_type_codes=[A]"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Sub-Agency 1",
            "abbreviation": "A1",
            "total_obligations": 101.0,
            "transaction_count": 1,
            "new_award_count": 1,
            "children": [
                {
                    "name": "Office 1",
                    "code": "0001",
                    "total_obligations": 101.0,
                    "transaction_count": 1,
                    "new_award_count": 1,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_agency_types(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&agency_type=funding"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Sub-Agency 1",
            "abbreviation": "A1",
            "total_obligations": 101.0,
            "transaction_count": 1,
            "new_award_count": 1,
            "children": [
                {
                    "name": "Office 2",
                    "code": "0002",
                    "total_obligations": 101.0,
                    "transaction_count": 1,
                    "new_award_count": 1,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_agency_without_office(client, monkeypatch, sub_agency_data_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="003", filter="?fiscal_year=2021&award_type_codes=[B]"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Sub-Agency 3",
            "abbreviation": "A3",
            "total_obligations": 110.0,
            "transaction_count": 1,
            "new_award_count": 1,
            "children": [],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, sub_agency_data_1):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
