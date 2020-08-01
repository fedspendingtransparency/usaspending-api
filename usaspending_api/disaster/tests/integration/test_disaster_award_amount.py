import pytest

from rest_framework import status

url = "/api/v2/disaster/award/amount/"


@pytest.mark.django_db
def test_award_amount_success(client, monkeypatch, generic_account_data, unlinked_faba_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_amount_endpoint(client, url, ["L"], ["A", "09", "10"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 1
    assert resp.data["outlay"] == 222
    assert resp.data["obligation"] == 200

    resp = helpers.post_for_amount_endpoint(client, url, ["N", "O"], ["A", "07", "08"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 2
    assert resp.data["outlay"] == 334
    assert resp.data["obligation"] == 4

    resp = helpers.post_for_amount_endpoint(client, url, ["9"], ["B"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 0
    assert resp.data["outlay"] == 0
    assert resp.data["obligation"] == 0


@pytest.mark.django_db
def test_award_amount_no_award_type_success(
    client, monkeypatch, generic_account_data, unlinked_faba_account_data, helpers
):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_amount_endpoint(client, url, ["N"], None)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 4
    assert resp.data["outlay"] == 10890108.00
    assert resp.data["obligation"] == 1088898.00

    resp = helpers.post_for_amount_endpoint(client, url, ["L", "M", "N", "O", "P"], None)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 7
    assert resp.data["outlay"] == 10890997.00
    assert resp.data["obligation"] == 1089204.00


@pytest.mark.django_db
def test_award_amount_on_sum_non_zero_toa(client, monkeypatch, multiple_file_c_to_same_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_amount_endpoint(client, url, ["M"], None)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 1
    assert resp.data["outlay"] == 0.0
    assert resp.data["obligation"] == 14.0


@pytest.mark.django_db
def test_award_amount_on_sum_non_zero_outlay(client, monkeypatch, multiple_outlay_file_c_to_same_award, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_amount_endpoint(client, url, ["M"], None)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 1
    assert resp.data["outlay"] == 14.0
    assert resp.data["obligation"] == 0.0


@pytest.mark.django_db
def test_award_amount_on_sum_zero_toa(client, monkeypatch, multiple_file_c_to_same_award_that_cancel_out, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_amount_endpoint(client, url, ["M"], None)
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 0
    assert resp.data["outlay"] == 0.0
    assert resp.data["obligation"] == 0.0


@pytest.mark.django_db
def test_award_amount_invalid_defc(client, monkeypatch, generic_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_amount_endpoint(client, url, ["ZZ"], ["A", "09", "10"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"


@pytest.mark.skip
@pytest.mark.django_db
def test_award_amount_exclusive_filters(client, helpers):
    resp = helpers.post_for_amount_endpoint(
        client, url, ["ZZ"], award_type_codes=["A", "09", "10"], award_type="procurement"
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_award_amount_bad_award_type_value(client, helpers):
    resp = helpers.post_for_amount_endpoint(client, url, ["ZZ"], award_type="financial")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_type_filters(client, monkeypatch, generic_account_data, unlinked_faba_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_amount_endpoint(client, url, ["L"], award_type="procurement")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 2
    assert resp.data["outlay"] == 777
    assert resp.data["obligation"] == 205

    resp = helpers.post_for_amount_endpoint(client, url, ["N", "O"], award_type="assistance")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["award_count"] == 3
    assert resp.data["outlay"] == 1222
    assert resp.data["obligation"] == 12
