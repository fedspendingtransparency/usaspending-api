import pytest
import datetime
from rest_framework import status

url = "/api/v2/disaster/federal_account/count/"


@pytest.mark.django_db
def test_federal_account_count_success(client, monkeypatch, disaster_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, datetime.datetime.now().year + 1, 12, 31)

    resp = helpers.post_for_count_endpoint(client, url, ["L", "M", "N", "O", "P"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 3

    resp = helpers.post_for_count_endpoint(client, url, ["N", "O"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 2

    resp = helpers.post_for_count_endpoint(client, url, ["P"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 1

    resp = helpers.post_for_count_endpoint(client, url, ["9"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_federal_account_count_invalid_defc(client, monkeypatch, disaster_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'L', 'M', 'N', 'O', 'P', 'Q']"


@pytest.mark.django_db
def test_federal_account_count_invalid_defc_type(client, monkeypatch, disaster_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, "100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_federal_account_count_missing_defc(client, monkeypatch, disaster_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
