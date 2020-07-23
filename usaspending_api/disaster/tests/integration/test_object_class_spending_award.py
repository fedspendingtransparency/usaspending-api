import pytest

from rest_framework import status

url = "/api/v2/disaster/object_class/spending/"


@pytest.mark.django_db
def test_basic_object_class_award_success(client, basic_faba_with_object_class, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": 1,
            "obligation": 0.0,
            "outlay": 0.0,
            "children": [
                {
                    "id": "1",
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": 1,
                    "obligation": 0.0,
                    "outlay": 0.0,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_object_class_counts_awards(client, faba_with_object_class_and_two_awards, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0]["award_count"] == 2
    assert len(resp.json()["results"][0]["children"]) == 1


@pytest.mark.django_db
def test_object_class_groups_by_object_classes(
    client, faba_with_two_object_classes_and_two_awards, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 2


@pytest.mark.django_db
def test_object_class_spending_filters_on_defc(client, basic_faba_with_object_class, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert len(resp.json()["results"]) == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_object_class_spending_filters_on_object_class_existance(
    client, award_count_sub_schedule, basic_faba, monkeypatch, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert len(resp.json()["results"]) == 0
