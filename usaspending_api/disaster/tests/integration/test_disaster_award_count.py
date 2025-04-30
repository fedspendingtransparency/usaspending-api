import pytest
from rest_framework import status

url = "/api/v2/disaster/award/count/"


@pytest.mark.django_db
def test_award_count_basic(client, basic_covid_faba_spending_data, helpers):
    helpers.reset_dabs_cache()
    resp = _default_post(client, helpers)
    assert resp.data["count"] == 1


@pytest.mark.django_db
def test_award_count_non_matching_defc(client, basic_covid_faba_spending_data, helpers):
    helpers.reset_dabs_cache()
    resp = helpers.post_for_count_endpoint(client, url, ["N"])
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_award_count_non_matching_award_type(client, basic_covid_faba_spending_data, helpers):
    helpers.reset_dabs_cache()
    resp = helpers.post_for_count_endpoint(client, url, ["M"], ["B"])
    assert resp.data["count"] == 0


@pytest.mark.django_db
def test_award_count_invalid_defc(client, basic_covid_faba_spending_data, helpers):
    resp = helpers.post_for_count_endpoint(client, url, ["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_count_invalid_defc_type(client, basic_covid_faba_spending_data, helpers):
    resp = helpers.post_for_count_endpoint(client, url, "100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_award_count_missing_defc(client, basic_covid_faba_spending_data, helpers):
    resp = helpers.post_for_count_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_award_count_award_type_codes_and_award_type_unprocessable(client, basic_covid_faba_spending_data):
    resp = client.post(
        url,
        content_type="application/json",
        data={"filter": {"def_codes": ["M"], "award_type_codes": ["A"], "award_type": "procurement"}},
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Cannot provide both 'award_type_codes' and 'award_type'"


def _default_post(client, helpers):
    return helpers.post_for_count_endpoint(client, url, ["M"], ["A"])
