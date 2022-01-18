import pytest
from rest_framework import status

from usaspending_api.awards.v2.views.accounts import SORTABLE_COLUMNS


FEDERAL_ACCOUNT_ENDPOINT = "/api/v2/awards/accounts/"


def _build_expected_response(request, award_id):
    sort_column = SORTABLE_COLUMNS[request.get("sort", "account_title")]
    sort_order = request.get("order", "desc")
    results = [
        {
            "account_title": f"federal_account_title_{2000 + 10 * award_id + _id}",
            "federal_account": f"{str(100 + award_id).zfill(3)}-{str(10 * award_id + _id).zfill(4)}",
            "funding_agency_abbreviation": None,
            "funding_agency_id": 9000 + award_id,
            "funding_agency_name": f"Toptier Funding Agency Name {9500 + award_id}",
            "funding_agency_slug": f"toptier-funding-agency-name-{9500 + award_id}",
            "funding_toptier_agency_id": 9500 + award_id,
            "total_transaction_obligated_amount": 10000 * (10 * award_id + _id) + (10 * award_id + _id),
        }
        for _id in range(1, award_id + 1)
    ]
    return {
        "results": sorted(results, key=lambda x: x[sort_column], reverse=sort_order == "desc"),
        "page_metadata": {
            "count": award_id,
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
        },
    }


def _test_post(client, request, award_id, expected_status_code=status.HTTP_200_OK):
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
    if response.status_code == status.HTTP_200_OK:
        assert response.json() == _build_expected_response(request, award_id)


@pytest.mark.django_db
def test_complete_queries(client, create_award_test_data):
    _test_post(client, {"award_id": 2}, 2)
    _test_post(client, {"award_id": 4}, 4)


@pytest.mark.django_db
def test_with_nonexistent_id(client, create_award_test_data):

    _test_post(client, {"award_id": 0}, 0)

    _test_post(client, {"award_id": "GENERATED_UNIQUE_AWARD_ID_000"}, 0)


@pytest.mark.django_db
def test_with_bogus_id(client, create_award_test_data):

    _test_post(client, {"award_id": "BOGUS_ID"}, 0)


@pytest.mark.django_db
def test_sort_columns(client, create_award_test_data):

    for sortable_column in SORTABLE_COLUMNS:

        _test_post(client, {"award_id": 2, "order": "desc", "sort": sortable_column}, 2)

        _test_post(client, {"award_id": 2, "order": "asc", "sort": sortable_column}, 2)

    _test_post(client, {"award_id": 2, "sort": "BOGUS FIELD"}, 0, expected_status_code=status.HTTP_400_BAD_REQUEST)
