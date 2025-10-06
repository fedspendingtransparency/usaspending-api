import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def assistance_listings_test_data():
    baker.make(
        "references.Cfda",
        program_number=10.001,
        program_title="CFDA Title 1",
    )
    baker.make(
        "references.Cfda",
        program_number=10.002,
        program_title="CFDA Title 2",
    )

    baker.make(
        "references.Cfda",
        program_number=10.003,
        program_title="CFDA Title 3",
    )

    baker.make(
        "references.Cfda",
        program_number=11.004,
        program_title="CFDA Title 1",
    )


@pytest.mark.django_db
def test_success(client, assistance_listings_test_data):
    resp = client.get("/api/v2/references/assistance_listing/", content_type="application/json")

    expected_results = [
        {
            "code": "10",
            "description": None,
            "count": 3,
            "children": [
                {"code": "10.001", "description": "CFDA Title 1"},
                {
                    "code": "10.002",
                    "description": "CFDA Title 2",
                },
                {
                    "code": "10.003",
                    "description": "CFDA Title 3",
                },
            ],
        },
        {
            "code": "11",
            "description": None,
            "count": 1,
            "children": [{"code": "11.004", "description": "CFDA Title 1"}],
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_with_code(client, assistance_listings_test_data):
    resp = client.get("/api/v2/references/assistance_listing/10/", content_type="application/json")

    expected_results = [
        {
            "code": "10",
            "description": None,
            "count": 3,
            "children": [
                {"code": "10.001", "description": "CFDA Title 1"},
                {
                    "code": "10.002",
                    "description": "CFDA Title 2",
                },
                {
                    "code": "10.003",
                    "description": "CFDA Title 3",
                },
            ],
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_with_filter(client, assistance_listings_test_data):
    resp = client.get("/api/v2/references/assistance_listing/10/?filter=Title 1", content_type="application/json")

    expected_results = [
        {
            "code": "10",
            "description": None,
            "count": 1,
            "children": [{"code": "10.001", "description": "CFDA Title 1"}],
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_filter_without_code(client, assistance_listings_test_data):
    resp = client.get("/api/v2/references/assistance_listing/?filter=Title 1", content_type="application/json")

    expected_results = [
        {
            "code": "10",
            "description": None,
            "count": 1,
            "children": [{"code": "10.001", "description": "CFDA Title 1"}],
        },
        {
            "code": "11",
            "description": None,
            "count": 1,
            "children": [{"code": "11.004", "description": "CFDA Title 1"}],
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_bad_code(client, assistance_listings_test_data):
    resp = client.get("/api/v2/references/assistance_listing/1/", content_type="application/json")

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "The assistance listing code should be two digits or not provided at all"
