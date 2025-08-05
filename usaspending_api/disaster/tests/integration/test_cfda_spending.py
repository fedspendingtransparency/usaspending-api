import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/cfda/spending/"


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
def test_correct_response_single_defc(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L"])
    expected_results = [
        {
            "code": "30.300",
            "award_count": 1,
            "description": "CFDA 3",
            "id": 300,
            "obligation": 2000.0,
            "outlay": 1000.0,
            "resource_link": "www.example.com/300",
            "applicant_eligibility": "AE3",
            "beneficiary_eligibility": "BE3",
            "cfda_federal_agency": "Agency 3",
            "cfda_objectives": "objectives 3",
            "cfda_website": "www.example.com/cfda_website/300",
        },
        {
            "code": "20.200",
            "award_count": 1,
            "description": "CFDA 2",
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
            "code": "30.300",
            "award_count": 1,
            "description": "CFDA 3",
            "id": 300,
            "obligation": 2000.0,
            "outlay": 1000.0,
            "resource_link": "www.example.com/300",
            "applicant_eligibility": "AE3",
            "beneficiary_eligibility": "BE3",
            "cfda_federal_agency": "Agency 3",
            "cfda_objectives": "objectives 3",
            "cfda_website": "www.example.com/cfda_website/300",
        },
        {
            "code": "20.200",
            "award_count": 2,
            "description": "CFDA 2",
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

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], query="3")
    expected_results = [
        {
            "code": "30.300",
            "award_count": 1,
            "description": "CFDA 3",
            "id": 300,
            "obligation": 2000.0,
            "outlay": 1000.0,
            "resource_link": "www.example.com/300",
            "applicant_eligibility": "AE3",
            "beneficiary_eligibility": "BE3",
            "cfda_federal_agency": "Agency 3",
            "cfda_objectives": "objectives 3",
            "cfda_website": "www.example.com/cfda_website/300",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_correct_response_with_award_type_codes(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], award_type_codes=["11"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], award_type_codes=["07", "09", "11"])
    expected_results = [
        {
            "code": "20.200",
            "award_count": 1,
            "description": "CFDA 2",
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


@pytest.mark.django_db
def test_pagination_page_and_limit(
    client, monkeypatch, helpers, elasticsearch_award_index, cfda_awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"], page=2, limit=1, sort="description")
    expected_results = {
        "totals": {"award_count": 4, "obligation": 2222.0, "outlay": 1100.0},
        "results": [
            {
                "code": "20.200",
                "award_count": 2,
                "description": "CFDA 2",
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
