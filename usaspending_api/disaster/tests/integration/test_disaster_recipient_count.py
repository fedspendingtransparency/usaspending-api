import pytest

from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/recipient/count/"


@pytest.mark.django_db
def test_award_count_basic_fabs(client, monkeypatch, basic_fabs_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["07"])
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_award_count_basic_fpds(client, monkeypatch, basic_fpds_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_wrong_award_type(client, monkeypatch, basic_fabs_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["B"])
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_no_award_type(client, monkeypatch, basic_fabs_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"])
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_two_transactions_two_awards(
    client, monkeypatch, basic_fabs_award, basic_fpds_award, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["A", "07"])
    assert resp.data["count"] == 2


@pytest.mark.django_db
def test_two_distinct_recipients(
    client, monkeypatch, double_fpds_awards_with_distinct_recipients, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 2


@pytest.mark.django_db
def test_two_same_recipients(
    client, monkeypatch, double_fpds_awards_with_same_recipients, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_zero_transactions_one_award(client, monkeypatch, award_with_no_outlays, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_fabs_quarterly(client, monkeypatch, fabs_award_with_quarterly_submission, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["07"])
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_fabs_old_submission(client, monkeypatch, fabs_award_with_old_submission, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["07"])
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_fabs_unclosed_submission(
    client, monkeypatch, fabs_award_with_unclosed_submission, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_award_count_invalid_defc(client, monkeypatch, basic_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, ["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_count_invalid_defc_type(client, monkeypatch, basic_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url, "100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_award_count_missing_defc(client, monkeypatch, basic_award, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_two_same_special_case_recipients(
    client, monkeypatch, double_fpds_awards_with_same_special_case_recipients, helpers, elasticsearch_award_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 1


def _default_post(client, helpers):
    return helpers.post_for_count_endpoint(client, url, ["M"], ["A"])
