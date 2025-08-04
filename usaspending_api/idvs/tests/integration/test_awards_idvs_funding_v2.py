import json
import pytest

from rest_framework import status
from usaspending_api.idvs.v2.views.funding import SORTABLE_COLUMNS

DETAIL_ENDPOINT = "/api/v2/idvs/funding/"


def _generate_expected_response(previous, next, page, has_previous, has_next, *award_ids):
    """
    Rather than manually generate an insane number of potential responses
    to test the various parameter combinations, we're going to procedurally
    generate them.  award_ids is the list of ids we expect back from the
    request in the order we expect them.  Unfortunately, for this to work,
    test data had to be generated in a specific way.  If you change how
    test data is generated you will probably also have to change this.
    """
    results = []
    for _id in award_ids:
        _sid = str(_id).zfill(3)
        results.append(
            {
                "award_id": _id,
                "generated_unique_award_id": "CONT_IDV_%s" % _sid,
                "gross_outlay_amount": 900.0 + _id,
                "reporting_fiscal_year": 2000 + _id,
                "reporting_fiscal_quarter": (_id % 12 + 3) // 3,
                "reporting_fiscal_month": _id % 12 + 1,
                "is_quarterly_submission": bool(_id % 2),
                "piid": "piid_%s" % _sid,
                "awarding_agency_id": 8000 + _id,
                "awarding_toptier_agency_id": 8500 + _id,
                "awarding_agency_name": "Toptier Awarding Agency Name %s" % (8500 + _id),
                "awarding_agency_slug": f"toptier-awarding-agency-name-{8500 + _id}",
                "disaster_emergency_fund_code": "A" if _id < 7 else None,
                "funding_agency_id": 9000 + _id,
                "funding_toptier_agency_id": 9500 + _id,
                "funding_agency_name": "Toptier Funding Agency Name %s" % (9500 + _id),
                "funding_agency_slug": f"toptier-funding-agency-name-{9500 + _id}",
                "agency_id": str(100 + _id).zfill(3),
                "main_account_code": _sid.zfill(4),
                "account_title": "federal_account_title_%s" % (2000 + _id),
                "program_activity_code": str(4000 + _id),
                "program_activity_name": "program_activity_%s" % (4000 + _id),
                "object_class": str(5000 + _id),
                "object_class_name": "object_class_%s" % (5000 + _id),
                "transaction_obligated_amount": 200000.0 + _id,
            }
        )

    page_metadata = {
        "previous": previous,
        "next": next,
        "page": page,
        "hasPrevious": has_previous,
        "hasNext": has_next,
    }

    return {"results": results, "page_metadata": page_metadata}


def _test_post(client, request, expected_response_parameters_tuple=None, expected_status_code=status.HTTP_200_OK):
    """
    Perform the actual request and interrogates the results.

    request is the Python dictionary that will be posted to the endpoint.
    expected_response_parameters are the values that you would normally
        pass into _generate_expected_response but we're going to do that
        for you so just pass the parameters as a tuple or list.
    expected_status_code is the HTTP status we expect to be returned from
        the call to the endpoint.

    Returns... nothing useful.
    """
    response = client.post(DETAIL_ENDPOINT, request)
    assert response.status_code == expected_status_code
    if expected_response_parameters_tuple is not None:
        expected_response = _generate_expected_response(*expected_response_parameters_tuple)
        assert json.loads(response.content.decode("utf-8")) == expected_response


@pytest.mark.django_db
def test_defaults(client, create_idv_test_data):

    _test_post(client, {"award_id": 1}, (None, None, 1, False, False, 6, 5, 4, 3, 1))
    _test_post(client, {"award_id": "CONT_IDV_001"}, (None, None, 1, False, False, 6, 5, 4, 3, 1))
    _test_post(client, {"award_id": 2}, (None, None, 1, False, False, 14, 13, 12, 11, 10, 9, 8, 7, 2))


@pytest.mark.django_db
def test_with_nonexistent_id(client):

    _test_post(client, {"award_id": 0}, (None, None, 1, False, False))
    _test_post(client, {"award_id": "CONT_IDV_000"}, (None, None, 1, False, False))


@pytest.mark.django_db
def test_with_bogus_id(client):

    _test_post(client, {"award_id": "BOGUS_ID"}, (None, None, 1, False, False))


@pytest.mark.django_db
def test_piid_filter(client, create_idv_test_data):

    _test_post(client, {"award_id": 2, "piid": "piid_013"}, (None, None, 1, False, False, 13))
    _test_post(client, {"award_id": 1, "piid": "nonexistent_piid"}, (None, None, 1, False, False))
    _test_post(client, {"award_id": 1, "piid": 12345}, (None, None, 1, False, False))


@pytest.mark.django_db
def test_limit_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 2, "limit": 1}, (None, 2, 1, False, True, 14))
    _test_post(client, {"award_id": 2, "limit": 10}, (None, None, 1, False, False, 14, 13, 12, 11, 10, 9, 8, 7, 2))
    _test_post(client, {"award_id": 2, "limit": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
    _test_post(client, {"award_id": 2, "limit": 2000000000}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
    _test_post(client, {"award_id": 2, "limit": {"BOGUS": "LIMIT"}}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_page_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 2, "limit": 1, "page": 2}, (1, 3, 2, True, True, 13))
    _test_post(client, {"award_id": 2, "limit": 1, "page": 9}, (8, None, 9, True, False, 2))

    # This should probably not be right, but it is the expected result.
    _test_post(client, {"award_id": 2, "limit": 1, "page": 99}, (98, None, 99, True, False))

    _test_post(
        client, {"award_id": 2, "limit": 1, "page": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )
    _test_post(
        client, {"award_id": 2, "limit": 1, "page": "BOGUS PAGE"}, expected_status_code=status.HTTP_400_BAD_REQUEST
    )


@pytest.mark.django_db
def test_sort_columns(client, create_idv_test_data):

    for sortable_column in SORTABLE_COLUMNS:

        _test_post(
            client,
            {"award_id": 2, "order": "desc", "sort": sortable_column},
            (None, None, 1, False, False, 14, 13, 12, 11, 10, 9, 8, 7, 2),
        )

        _test_post(
            client,
            {"award_id": 2, "order": "asc", "sort": sortable_column},
            (None, None, 1, False, False, 2, 7, 8, 9, 10, 11, 12, 13, 14),
        )

    _test_post(client, {"award_id": 2, "sort": "BOGUS FIELD"}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_sort_order_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 2, "order": "desc"}, (None, None, 1, False, False, 14, 13, 12, 11, 10, 9, 8, 7, 2))
    _test_post(client, {"award_id": 2, "order": "asc"}, (None, None, 1, False, False, 2, 7, 8, 9, 10, 11, 12, 13, 14))
    _test_post(client, {"award_id": 2, "order": "BOGUS ORDER"}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_complete_queries(client, create_idv_test_data):

    _test_post(
        client,
        {"award_id": 2, "piid": "piid_013", "limit": 3, "page": 1, "sort": "piid", "order": "asc"},
        (None, None, 1, False, False, 13),
    )
