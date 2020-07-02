import pytest

from rest_framework import status

url = "/api/v2/disaster/award/amount/"


@pytest.mark.django_db
def test_agency_count_success(client, monkeypatch, account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_amount_endpoint(client, url, ["L"], ["A", "09", "10"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 1
    assert resp.data["outlay"] == 222
    assert resp.data["obligation"] == 200

    resp = helpers.post_for_amount_endpoint(client, url, ["N", "O"], ["A", "09", "10"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 2
    assert resp.data["outlay"] == 334
    assert resp.data["obligation"] == 4

    resp = helpers.post_for_amount_endpoint(client, url, ["9"], ["B"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 0
    assert resp.data["outlay"] == 0
    assert resp.data["obligation"] == 0


@pytest.mark.django_db
def test_agency_count_invalid_defc(client, monkeypatch, account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_amount_endpoint(client, url, ["ZZ"], ["A", "09", "10"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"
