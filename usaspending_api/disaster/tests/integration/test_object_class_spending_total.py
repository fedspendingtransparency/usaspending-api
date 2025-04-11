import pytest
from rest_framework import status

url = "/api/v2/disaster/object_class/spending/"


@pytest.mark.django_db
def test_basic_object_class_spending_total_success(
    client, basic_fa_by_object_class_with_object_class, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "N", "O"], spending_type="total")
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": None,
            "obligation": 1732.0,
            "outlay": 1190.0,
            "children": [
                {
                    "id": "3",
                    "code": "0003",
                    "description": "0003 name",
                    "award_count": None,
                    "obligation": 1180.0,
                    "outlay": 1190.0,
                },
                {
                    "id": "2",
                    "code": "0002",
                    "description": "0002 name",
                    "award_count": None,
                    "obligation": 345.0,
                    "outlay": 0.0,
                },
                {
                    "id": "1",
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": None,
                    "obligation": 207.0,
                    "outlay": 0.0,
                },
            ],
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"obligation": 1732.0, "outlay": 1190.0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_object_class_spending_filters_on_defc(
    client, basic_fa_by_object_class_with_object_class, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="total")
    assert len(resp.json()["results"]) == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="total")
    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_object_class_spending_filters_on_non_zero_obligations(
    client, basic_fa_by_object_class_with_object_class_but_no_obligations, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="total")
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_object_class_spending_adds_over_multiple_object_classes(
    client, basic_fa_by_object_class_with_multpile_object_class, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="total")
    assert len(resp.json()["results"]) == 1
    assert len(resp.json()["results"][0]["children"]) == 3
    assert resp.json()["results"][0]["obligation"] == 11
    assert resp.json()["results"][0]["outlay"] == 22


@pytest.mark.django_db
def test_object_class_spending_adds_over_multiple_object_classes_of_same_code(
    client, basic_fa_by_object_class_with_multpile_object_class_of_same_code, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="total")
    assert len(resp.json()["results"]) == 1
    assert len(resp.json()["results"][0]["children"]) == 1
    assert resp.json()["results"][0]["obligation"] == 10
    assert resp.json()["results"][0]["outlay"] == 22
