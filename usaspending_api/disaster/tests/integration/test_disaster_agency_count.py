import pytest

from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/agency/count/"


@pytest.mark.django_db
def test_agency_count_success(client, monkeypatch, disaster_account_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_count_endpoint(client, url, ["L", "M", "N", "O", "P"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 3

    resp = helpers.post_for_count_endpoint(client, url, ["N", "O"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 3

    resp = helpers.post_for_count_endpoint(client, url, ["P"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 1

    resp = helpers.post_for_count_endpoint(client, url, ["9"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_agency_count_with_award_types(
    client, monkeypatch, faba_with_toptier_agencies, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["A"])
    assert resp.data["count"] == 2


@pytest.mark.django_db
def test_agency_ignores_agencies_with_zero_sum_toa(
    client, monkeypatch, faba_with_toptier_agencies_that_cancel_out_in_toa, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["A"])
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_agency_ignores_agencies_with_zero_sum_outlay(
    client, monkeypatch, faba_with_toptier_agencies_that_cancel_out_in_outlay, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["A"])
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_agency_count_invalid_defc(client, monkeypatch, disaster_account_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'L', 'M', 'N', 'O', 'P', 'Q']"


@pytest.mark.django_db
def test_agency_count_invalid_defc_type(client, monkeypatch, disaster_account_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, "100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_agency_count_missing_defc(client, monkeypatch, disaster_account_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
