import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/cfda/loans/"


@pytest.mark.django_db
def test_correct_response_defc_no_results(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, award_type_codes=["07", "08"], def_codes=["N"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_single_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L"])
    expected_results = [
        {
            "code": "20.200",
            "award_count": 1,
            "description": "CFDA 2",
            "face_value_of_loan": 30.0,
            "id": 200,
            "obligation": 20.0,
            "outlay": 0.0,
            "resource_link": "www.example.com/200",
            "applicant_eligibility": "AE2",
            "beneficiary_eligibility": "BE2",
            "cfda_federal_agency": "Agency 2",
            "cfda_objectives": "objectives 2",
            "cfda_website": "www.example.com/cfda_website/200",
        },
        {
            "code": "10.100",
            "award_count": 1,
            "description": "CFDA 1",
            "face_value_of_loan": 3.0,
            "id": 100,
            "obligation": 2.0,
            "outlay": 0.0,
            "resource_link": None,
            "applicant_eligibility": "AE1",
            "beneficiary_eligibility": "BE1",
            "cfda_federal_agency": "Agency 1",
            "cfda_objectives": "objectives 1",
            "cfda_website": None,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_multiple_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"])
    expected_results = [
        {
            "code": "20.200",
            "award_count": 2,
            "description": "CFDA 2",
            "face_value_of_loan": 330.0,
            "id": 200,
            "obligation": 220.0,
            "outlay": 100.0,
            "resource_link": "www.example.com/200",
            "applicant_eligibility": "AE2",
            "beneficiary_eligibility": "BE2",
            "cfda_federal_agency": "Agency 2",
            "cfda_objectives": "objectives 2",
            "cfda_website": "www.example.com/cfda_website/200",
        },
        {
            "code": "10.100",
            "award_count": 1,
            "description": "CFDA 1",
            "face_value_of_loan": 3.0,
            "id": 100,
            "obligation": 2.0,
            "outlay": 0.0,
            "resource_link": None,
            "applicant_eligibility": "AE1",
            "beneficiary_eligibility": "BE1",
            "cfda_federal_agency": "Agency 1",
            "cfda_objectives": "objectives 1",
            "cfda_website": None,
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_with_query(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="GIBBERISH")
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="2")
    expected_results = [
        {
            "code": "20.200",
            "award_count": 2,
            "description": "CFDA 2",
            "face_value_of_loan": 330.0,
            "id": 200,
            "obligation": 220.0,
            "outlay": 100.0,
            "resource_link": "www.example.com/200",
            "applicant_eligibility": "AE2",
            "beneficiary_eligibility": "BE2",
            "cfda_federal_agency": "Agency 2",
            "cfda_objectives": "objectives 2",
            "cfda_website": "www.example.com/cfda_website/200",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_defc(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['L', 'M', 'N']"


@pytest.mark.django_db
def test_invalid_defc_type(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_missing_defc(client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_pagination_page_and_limit(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], page=2, limit=1)
    expected_results = {
        "totals": {"award_count": 3, "face_value_of_loan": 333.0, "obligation": 222.0, "outlay": 100.0},
        "results": [
            {
                "code": "10.100",
                "award_count": 1,
                "description": "CFDA 1",
                "face_value_of_loan": 3.0,
                "id": 100,
                "obligation": 2.0,
                "outlay": 0.0,
                "resource_link": None,
                "applicant_eligibility": "AE1",
                "beneficiary_eligibility": "BE1",
                "cfda_federal_agency": "Agency 1",
                "cfda_objectives": "objectives 1",
                "cfda_website": None,
            }
        ],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": True,
            "limit": 1,
            "next": None,
            "page": 2,
            "previous": 1,
            "total": 2,
        },
        "messages": [
            "Notice! API Request to sort on 'id' field isn't fully "
            "implemented. Results were actually sorted using 'description' "
            "field."
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, award_type_codes=["ZZ", "08"], def_codes=["L", "M"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|award_type_codes' is outside valid values ['07', '08']"


@pytest.mark.django_db
def test_correct_response_with_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["07"], def_codes=["L", "M"], sort="description"
    )
    expected_results = {
        "totals": {"award_count": 2, "face_value_of_loan": 33.0, "obligation": 22.0, "outlay": 0.0},
        "results": [
            {
                "code": "20.200",
                "award_count": 1,
                "description": "CFDA 2",
                "face_value_of_loan": 30.0,
                "id": 200,
                "obligation": 20.0,
                "outlay": 0.0,
                "resource_link": "www.example.com/200",
                "applicant_eligibility": "AE2",
                "beneficiary_eligibility": "BE2",
                "cfda_federal_agency": "Agency 2",
                "cfda_objectives": "objectives 2",
                "cfda_website": "www.example.com/cfda_website/200",
            },
            {
                "code": "10.100",
                "award_count": 1,
                "description": "CFDA 1",
                "face_value_of_loan": 3.0,
                "id": 100,
                "obligation": 2.0,
                "outlay": 0.0,
                "resource_link": None,
                "applicant_eligibility": "AE1",
                "beneficiary_eligibility": "BE1",
                "cfda_federal_agency": "Agency 1",
                "cfda_objectives": "objectives 1",
                "cfda_website": None,
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
        "totals": {"award_count": 1, "face_value_of_loan": 300.0, "obligation": 200.0, "outlay": 100.0},
        "results": [
            {
                "code": "20.200",
                "award_count": 1,
                "description": "CFDA 2",
                "face_value_of_loan": 300.0,
                "id": 200,
                "obligation": 200.0,
                "outlay": 100.0,
                "resource_link": "www.example.com/200",
                "applicant_eligibility": "AE2",
                "beneficiary_eligibility": "BE2",
                "cfda_federal_agency": "Agency 2",
                "cfda_objectives": "objectives 2",
                "cfda_website": "www.example.com/cfda_website/200",
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
