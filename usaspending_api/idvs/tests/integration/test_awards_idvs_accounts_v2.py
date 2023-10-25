import pytest
from rest_framework import status

from usaspending_api.idvs.tests.data.idv_test_data import AWARD_COUNT
from usaspending_api.idvs.v2.views.accounts import SORTABLE_COLUMNS


FEDERAL_ACCOUNT_ENDPOINT = "/api/v2/idvs/accounts/"


def _test_post(client, request, expected_status_code=status.HTTP_200_OK):
    """
    Perform the actual request and interrogates the results.

    request is the Python dictionary that will be posted to the endpoint.
    expected_response_parameters are the values that you would normally
        pass into _generate_expected_response but we're going to do that
        for you so just pass the parameters as a tuple or list.
    expected_status_code is the HTTP status we expect to be returned from
        the call to the endpoint.
    """
    response = client.post(FEDERAL_ACCOUNT_ENDPOINT, request)
    assert response.status_code == expected_status_code


@pytest.mark.django_db
def test_complete_queries(client):
    for _id in range(1, AWARD_COUNT + 1):
        _test_post(client, {"award_id": _id})


@pytest.mark.django_db
def test_with_nonexistent_id(client):

    _test_post(client, {"award_id": 0})

    _test_post(client, {"award_id": "CONT_IDV_000"})


@pytest.mark.django_db
def test_with_bogus_id(client):

    _test_post(client, {"award_id": "BOGUS_ID"})


@pytest.mark.django_db
def test_sort_columns(client):

    for sortable_column in SORTABLE_COLUMNS:

        _test_post(client, {"award_id": 2, "order": "desc", "sort": sortable_column})

        _test_post(client, {"award_id": 2, "order": "asc", "sort": sortable_column})

    _test_post(client, {"award_id": 2, "sort": "BOGUS FIELD"}, expected_status_code=status.HTTP_400_BAD_REQUEST)
