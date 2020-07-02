import pytest

from rest_framework import status

url = "/api/v2/disaster/award/count/"


@pytest.mark.django_db
def test_award_count_basic(client, monkeypatch, basic_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"])
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_award_count_invalid_defc(client, monkeypatch, basic_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_count_invalid_defc_type(client, monkeypatch, basic_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, "100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_award_count_missing_defc(client, monkeypatch, basic_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
