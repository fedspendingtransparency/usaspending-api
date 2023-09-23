import json
import pytest

from django.utils.text import slugify
from model_bakery import baker
from rest_framework import status
from usaspending_api.idvs.tests.data.idv_test_data import IDVS, PARENTS
from usaspending_api.idvs.v2.views.awards import SORTABLE_COLUMNS
from usaspending_api.submissions.models.submission_attributes import SubmissionAttributes


ENDPOINT = "/api/v2/idvs/awards/"


def _generate_expected_response(previous, next, page, has_previous, has_next, no_submissions, *award_ids):
    """
    Rather than manually generate an insane number of potential responses
    to test the various parameter combinations, we're going to procedurally
    generate them.  award_ids is the list of ids we expect back from the
    request in the order we expect them.  Unfortunately, for this to work,
    test data had to be generated in a specific way.  If you change how
    test data is generated you will probably also have to change this.
    """
    results = []
    for award_id in award_ids:
        string_award_id = str(award_id).zfill(3)
        funding_agency_name = "Toptier Funding Agency Name %s" % (9500 + award_id)
        awarding_agency_name = "Toptier Awarding Agency Name %s" % (8500 + award_id)
        funding_agency_slug = slugify(funding_agency_name) if not no_submissions else None
        awarding_agency_slug = slugify(awarding_agency_name) if not no_submissions else None
        results.append(
            {
                "award_id": award_id,
                "award_type": "type_description_%s" % string_award_id,
                "awarding_agency": awarding_agency_name,
                "awarding_agency_slug": awarding_agency_slug,
                "awarding_agency_id": 8000 + award_id,
                "description": "description_%s" % string_award_id,
                "funding_agency": funding_agency_name,
                "funding_agency_slug": funding_agency_slug,
                "funding_agency_id": 9000 + award_id,
                "generated_unique_award_id": "CONT_IDV_%s" % string_award_id,
                "last_date_to_order": "2018-01-%02d" % award_id,
                "obligated_amount": (300000 if award_id in IDVS else 100000) + award_id,
                "period_of_performance_current_end_date": "2018-03-%02d" % award_id,
                "period_of_performance_start_date": "2018-02-%02d" % award_id,
                "piid": "piid_%s" % string_award_id,
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
    response = client.post(ENDPOINT, request)
    assert response.status_code == expected_status_code
    if expected_response_parameters_tuple is not None:
        expected_response = _generate_expected_response(*expected_response_parameters_tuple)
        assert json.loads(response.content.decode("utf-8")) == expected_response


@pytest.mark.django_db
def test_defaults(client, create_idv_test_data):

    _test_post(client, {"award_id": 1}, (None, None, 1, False, False, False, 5, 4, 3))

    _test_post(client, {"award_id": "CONT_IDV_001"}, (None, None, 1, False, False, False, 5, 4, 3))


@pytest.mark.django_db
def test_with_nonexistent_id(client):

    _test_post(client, {"award_id": 0}, (None, None, 1, False, False, False))

    _test_post(client, {"award_id": "CONT_IDV_000"}, (None, None, 1, False, False, False))


@pytest.mark.django_db
def test_with_bogus_id(client):

    _test_post(client, {"award_id": "BOGUS_ID"}, (None, None, 1, False, False, None))


@pytest.mark.django_db
def test_type(client, create_idv_test_data):

    _test_post(client, {"award_id": 1, "type": "child_idvs"}, (None, None, 1, False, False, False, 5, 4, 3))

    _test_post(client, {"award_id": 1, "type": "child_awards"}, (None, None, 1, False, False, False, 6))

    _test_post(client, {"award_id": 1, "type": "grandchild_awards"}, (None, None, 1, False, False, False))

    _test_post(
        client, {"award_id": 2, "type": "grandchild_awards"}, (None, None, 1, False, False, False, 14, 13, 12, 11)
    )

    _test_post(client, {"award_id": 1, "type": "BOGUS TYPE"}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_limit_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 1, "limit": 1}, (None, 2, 1, False, True, False, 5))

    _test_post(client, {"award_id": 1, "limit": 5}, (None, None, 1, False, False, False, 5, 4, 3))

    _test_post(client, {"award_id": 1, "limit": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

    _test_post(client, {"award_id": 1, "limit": 2000000000}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

    _test_post(client, {"award_id": 1, "limit": {"BOGUS": "LIMIT"}}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_page_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 1, "limit": 1, "page": 2}, (1, 3, 2, True, True, False, 4))

    _test_post(client, {"award_id": 1, "limit": 1, "page": 3}, (2, None, 3, True, False, False, 3))

    _test_post(client, {"award_id": 1, "limit": 1, "page": 4}, (3, None, 4, True, False, False))

    _test_post(
        client, {"award_id": 1, "limit": 1, "page": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )

    _test_post(
        client, {"award_id": 1, "limit": 1, "page": "BOGUS PAGE"}, expected_status_code=status.HTTP_400_BAD_REQUEST
    )


@pytest.mark.django_db
def test_sort_columns(client, create_idv_test_data):

    for sortable_column in SORTABLE_COLUMNS:

        _test_post(
            client,
            {"award_id": 1, "order": "desc", "sort": sortable_column},
            (None, None, 1, False, False, False, 5, 4, 3),
        )

        _test_post(
            client,
            {"award_id": 1, "order": "asc", "sort": sortable_column},
            (None, None, 1, False, False, False, 3, 4, 5),
        )

    _test_post(client, {"award_id": 1, "sort": "BOGUS FIELD"}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_sort_order_values(client, create_idv_test_data):

    _test_post(client, {"award_id": 1, "order": "desc"}, (None, None, 1, False, False, False, 5, 4, 3))

    _test_post(client, {"award_id": 1, "order": "asc"}, (None, None, 1, False, False, False, 3, 4, 5))

    _test_post(client, {"award_id": 1, "order": "BOGUS ORDER"}, expected_status_code=status.HTTP_400_BAD_REQUEST)


@pytest.mark.django_db
def test_complete_queries(client, create_idv_test_data):

    _test_post(
        client,
        {"award_id": 1, "type": "child_idvs", "limit": 3, "page": 1, "sort": "description", "order": "asc"},
        (None, None, 1, False, False, False, 3, 4, 5),
    )

    _test_post(
        client,
        {"award_id": 1, "type": "child_awards", "limit": 3, "page": 1, "sort": "description", "order": "asc"},
        (None, None, 1, False, False, False, 6),
    )


@pytest.mark.django_db
def test_no_grandchildren_returned(client, create_idv_test_data):

    _test_post(client, {"award_id": 2, "type": "child_idvs"}, (None, None, 1, False, False, False, 8, 7))

    _test_post(client, {"award_id": 2, "type": "child_awards"}, (None, None, 1, False, False, False, 10, 9))


@pytest.mark.django_db
def test_no_parents_returned(client, create_idv_test_data):

    _test_post(client, {"award_id": 7, "type": "child_idvs"}, (None, None, 1, False, False, False))

    _test_post(client, {"award_id": 7, "type": "child_awards"}, (None, None, 1, False, False, False, 12, 11))


@pytest.mark.django_db
def test_nothing_returned_for_bogus_contract_relationship(client):

    _test_post(client, {"award_id": 9, "type": "child_idvs"}, (None, None, 1, False, False, False))

    _test_post(client, {"award_id": 9, "type": "child_awards"}, (None, None, 1, False, False, False))


@pytest.mark.django_db
def test_missing_agency(client, create_idv_test_data):
    # A bug was found where awards wouldn't show up if the funding agency was
    # null.  This will reproduce that situation.
    award_id = 999
    string_award_id = str(award_id).zfill(3)

    parent_award_id = PARENTS.get(3)  # Use use parent information for I3
    string_parent_award_id = str(parent_award_id).zfill(3)

    baker.make(
        "search.TransactionSearch",
        transaction_id=award_id,
        award_id=award_id,
        funding_toptier_agency_name="subtier_funding_agency_name_%s" % string_award_id,
        awarding_toptier_agency_name="subtier_awarding_agency_name_%s" % string_award_id,
        is_fpds=True,
    )

    baker.make(
        "search.AwardSearch",
        award_id=award_id,
        generated_unique_award_id="CONT_IDV_%s" % string_award_id,
        type="CONTRACT_%s" % string_award_id,
        total_obligation=award_id,
        piid="piid_%s" % string_award_id,
        fpds_agency_id="fpds_agency_id_%s" % string_award_id,
        parent_award_piid="piid_%s" % string_parent_award_id,
        fpds_parent_agency_id="fpds_agency_id_%s" % string_parent_award_id,
        latest_transaction_id=award_id,
        type_description="type_description_%s" % string_award_id,
        description="description_%s" % string_award_id,
        period_of_performance_current_end_date="2018-03-28",
        period_of_performance_start_date="2018-02-28",
    )

    response = client.post(ENDPOINT, {"award_id": parent_award_id, "type": "child_awards"})

    # This should return two results.  Prior to the bug, only one result would be returned.
    assert len(response.data["results"]) == 2


@pytest.mark.django_db
def test_no_submission(client, create_idv_test_data):
    SubmissionAttributes.objects.filter(reporting_fiscal_year=2008).delete()
    _test_post(client, {"award_id": 1, "type": "child_idvs"}, (None, None, 1, False, False, True, 5, 4, 3))
    _test_post(client, {"award_id": 1, "type": "child_awards"}, (None, None, 1, False, False, True, 6))
    _test_post(client, {"award_id": 1, "type": "grandchild_awards"}, (None, None, 1, False, False, True))
