import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/recipient/loans/"


def _get_shape_code_for_sort(result_dict):
    return result_dict["shape_code"]


@pytest.mark.django_db
def test_correct_response_defc_no_results(
    client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["N"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_single_defc(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L"])
    expected_results = [
        {
            "code": "987654321",
            "award_count": 1,
            "description": "RECIPIENT 3",
            "face_value_of_loan": 300.0,
            "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
            "obligation": 200.0,
            "outlay": 100.0,
        },
        {
            "code": "456789123",
            "award_count": 1,
            "description": "RECIPIENT 2",
            "face_value_of_loan": 30.0,
            "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
            "obligation": 20.0,
            "outlay": 10.0,
        },
        {
            "code": "DUNS Number not provided",
            "award_count": 1,
            "description": "RECIPIENT 1",
            "face_value_of_loan": 3.0,
            "id": ["5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R"],
            "obligation": 2.0,
            "outlay": 1.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_multiple_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"])
    expected_results = [
        {
            "code": "987654321",
            "award_count": 1,
            "description": "RECIPIENT 3",
            "face_value_of_loan": 300.0,
            "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
            "obligation": 200.0,
            "outlay": 100.0,
        },
        {
            "code": "456789123",
            "award_count": 1,
            "description": "RECIPIENT 2",
            "face_value_of_loan": 30.0,
            "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
            "obligation": 20.0,
            "outlay": 10.0,
        },
        {
            "code": "DUNS Number not provided",
            "award_count": 1,
            "description": "RECIPIENT 1",
            "face_value_of_loan": 3.0,
            "id": ["5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R"],
            "obligation": 2.0,
            "outlay": 1.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_with_query(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="GIBBERISH")
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="3")
    expected_results = [
        {
            "code": "987654321",
            "award_count": 1,
            "description": "RECIPIENT 3",
            "face_value_of_loan": 300.0,
            "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
            "obligation": 200.0,
            "outlay": 100.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="REC")
    expected_results = [
        {
            "code": "987654321",
            "award_count": 1,
            "description": "RECIPIENT 3",
            "face_value_of_loan": 300.0,
            "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
            "obligation": 200.0,
            "outlay": 100.0,
        },
        {
            "code": "456789123",
            "award_count": 1,
            "description": "RECIPIENT 2",
            "face_value_of_loan": 30.0,
            "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
            "obligation": 20.0,
            "outlay": 10.0,
        },
        {
            "code": "DUNS Number not provided",
            "award_count": 1,
            "description": "RECIPIENT 1",
            "face_value_of_loan": 3.0,
            "id": ["5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R"],
            "obligation": 2.0,
            "outlay": 1.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="rec")
    expected_results = [
        {
            "code": "987654321",
            "award_count": 1,
            "description": "RECIPIENT 3",
            "face_value_of_loan": 300.0,
            "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
            "obligation": 200.0,
            "outlay": 100.0,
        },
        {
            "code": "456789123",
            "award_count": 1,
            "description": "RECIPIENT 2",
            "face_value_of_loan": 30.0,
            "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
            "obligation": 20.0,
            "outlay": 10.0,
        },
        {
            "code": "DUNS Number not provided",
            "award_count": 1,
            "description": "RECIPIENT 1",
            "face_value_of_loan": 3.0,
            "id": ["5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R"],
            "obligation": 2.0,
            "outlay": 1.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_defc(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['L', 'M', 'N']"


@pytest.mark.django_db
def test_invalid_defc_type(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_missing_defc(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_pagination_page_and_limit(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], page=2, limit=1, sort="description")
    expected_results = {
        "results": [
            {
                "code": "456789123",
                "award_count": 1,
                "description": "RECIPIENT 2",
                "face_value_of_loan": 30.0,
                "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
                "obligation": 20.0,
                "outlay": 10.0,
            }
        ],
        "page_metadata": {
            "hasNext": True,
            "hasPrevious": True,
            "limit": 1,
            "next": 3,
            "page": 2,
            "previous": 1,
            "total": 3,
        },
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_award_type_codes(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, award_type_codes=["ZZ", "08"], def_codes=["L", "M"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|award_type_codes' is outside valid values ['07', '08']"


@pytest.mark.django_db
def test_correct_response_with_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["07"], def_codes=["L", "M"], sort="description"
    )
    expected_results = {
        "results": [
            {
                "code": "456789123",
                "award_count": 1,
                "description": "RECIPIENT 2",
                "face_value_of_loan": 30.0,
                "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
                "obligation": 20.0,
                "outlay": 10.0,
            },
            {
                "code": "DUNS Number not provided",
                "award_count": 1,
                "description": "RECIPIENT 1",
                "face_value_of_loan": 3.0,
                "id": ["5f572ec9-8b49-e5eb-22c7-f6ef316f7689-R"],
                "obligation": 2.0,
                "outlay": 1.0,
            },
        ],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 2,
        },
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["08"], def_codes=["L", "M"], sort="description"
    )
    expected_results = {
        "results": [
            {
                "code": "987654321",
                "award_count": 1,
                "description": "RECIPIENT 3",
                "face_value_of_loan": 300.0,
                "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
                "obligation": 200.0,
                "outlay": 100.0,
            }
        ],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results
