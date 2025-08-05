import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/cfda/count/"


@pytest.mark.django_db
def test_correct_response_defc_no_results(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url, def_codes=["N"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["count"] == 0


@pytest.mark.django_db
def test_correct_response_single_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url, def_codes=["L"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["count"] == 3


@pytest.mark.django_db
def test_correct_response_multiple_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url, def_codes=["L", "M"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["count"] == 3


@pytest.mark.django_db
def test_correct_response_with_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], award_type_codes=["11"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["count"] == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], award_type_codes=["07", "09", "11"])
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["count"] == 2


@pytest.mark.django_db
def test_invalid_defc(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['L', 'M', 'N']"


@pytest.mark.django_db
def test_invalid_defc_type(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_missing_defc(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_invalid_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, award_type_codes=["ZZ", "08"], def_codes=["L", "M"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.data["detail"]
        == "Field 'filter|award_type_codes' is outside valid values ['-1', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']"
    )
