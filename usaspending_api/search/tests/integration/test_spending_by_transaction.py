import json
from time import perf_counter

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_transaction/"


@pytest.fixture
def transaction_data():
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_description="test",
        recipient_location_zip5="abcde",
        piid="IND12PB00323",
        recipient_uei="testuei",
        parent_uei="test_parent_uei",
        action_type="A",
        legal_entity_address_line1="test address line",
        legal_entity_address_line2="address2",
        legal_entity_address_line3="address3",
        legal_entity_zip_last4="6789",
        legal_entity_foreign_posta="foreignpostalcode",
        legal_entity_foreign_provi="foreignprovince",
        recipient_location_country_code="USA",
        recipient_location_state_code="TX",
        recipient_location_country_name="UNITED STATES",
        recipient_location_county_code="001",
        recipient_location_county_name="testcountyname",
        recipient_location_congressional_code="congressionalcode",
        recipient_location_city_name="cityname",
        pop_country_code="popcountrycode",
        pop_country_name="UNITED STATES",
        pop_state_code="TX",
        pop_city_name="popcityname",
        pop_county_code="popcountycode",
        pop_county_name="popcountyname",
        pop_congressional_code="popcongressionalcode",
        place_of_perform_zip_last4="popziplast4",
        pop_zip5="popzip5",
        naics_code="naicscode",
        naics_description="naicsdescription",
        product_or_service_code="psccode",
        product_or_service_description="pscdescription",
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        action_date="2010-10-01",
        is_fpds=True,
        type="10",
        transaction_description="award 1",
        federal_action_obligation=35.00,
        recipient_location_zip5="abcde",
        piid="IND12PB00001",
        recipient_uei="testuei",
        parent_uei="test_parent_uei",
        generated_unique_award_id="IASST_NON_WY99M000020-18Z_8630",
        cfda_number="59",
        cfda_title="cfdatitle",
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        display_award_id="IND12PB00323",
        latest_transaction_id=2,
        is_fpds=True,
        type="10",
        piid="IND12PB00323",
    )
    award1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
    )
    ref_program_activity1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code=123,
        program_activity_name="PROGRAM_ACTIVITY_123",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award=award1,
        program_activity_id=ref_program_activity1.id,
    )


@pytest.mark.django_db
def test_spending_by_transaction_kws_success(client, elasticsearch_transaction_index):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
                "fields": ["Award ID", "Recipient Name", "Mod"],
                "page": 1,
                "limit": 5,
                "sort": "Award ID",
                "order": "desc",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_transaction_kws_failure(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps({"filters": {}}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_no_intersection(client):
    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D", "no intersection"]},
        "fields": ["Award ID", "Recipient Name", "Mod"],
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }
    api_start = perf_counter()

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))
    api_end = perf_counter()
    assert resp.status_code == status.HTTP_200_OK
    assert api_end - api_start < 0.5, "Response took over 0.5s! Investigate why"
    assert len(resp.data["results"]) == 0, "Results returned, there should be 0"


@pytest.mark.django_db
def test_all_fields_returned(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Recipient Name",
        "Action Date",
        "Transaction Amount",
        "Award Type",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Funding Agency",
        "Funding Sub Agency",
        "Issued Date",
        "Loan Value",
        "Subsidy Cost",
        "Mod",
        "Award ID",
        "awarding_agency_id",
        "internal_id",
        "generated_internal_id",
        "Last Date to Order",
        "Transaction Description",
        "Action Type",
        "Recipient UEI",
        "Recipient Location",
        "Primary Place of Performance",
        "NAICS",
        "PSC",
        "Assistance Listing",
    ]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) > 0
    for result in resp.data["results"]:
        for field in fields:
            assert field in result, f"Response item is missing field {field}"

        assert "Sausage" not in result
        assert "A" not in result


@pytest.mark.django_db
def test_subset_of_fields_returned(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID", "Recipient Name", "Mod"]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) > 0
    for result in resp.data["results"]:
        for field in fields:
            assert field in result, f"Response item is missing field {field}"

        assert "internal_id" in result
        assert "generated_internal_id" in result
        assert "Last Date to Order" not in result


@pytest.mark.django_db
def test_columns_can_be_sorted(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Action Date",
        "Award ID",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Award Type",
        "Mod",
        "Recipient Name",
        "Action Date",
    ]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "order": "desc",
    }

    for field in fields:
        request["sort"] = field
        resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))
        assert resp.status_code == status.HTTP_200_OK, f"Failed to sort column: {field}"


@pytest.mark.django_db
def test_uei(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID"]

    request = {
        "filters": {"keyword": "testuei", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_parent_uei(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID"]

    request = {
        "filters": {"keyword": "test_parent_uei", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_txn_program_activity(client, monkeypatch, elasticsearch_transaction_index, transaction_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Program Activites filter test
    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = []

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_transaction_award_unique_id_filter(
    client, monkeypatch, elasticsearch_transaction_index, transaction_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "award_unique_id": "ASST_NON_WY99M000020-18Z_8630"},
    }

    expected_response = [
        {"Award ID": "IND12PB00001", "generated_internal_id": "ASST_NON_WY99M000020-18Z_8630", "internal_id": 2}
    ]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 1
    assert expected_response == results


@pytest.mark.django_db
def test_additional_fields(client, monkeypatch, elasticsearch_transaction_index, transaction_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Award ID",
        "Transaction Description",
        "Action Type",
        "Recipient UEI",
        "Recipient Location",
        "Primary Place of Performance",
        "NAICS",
        "PSC",
    ]

    request = {
        "filters": {
            "keyword": "test",
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) > 0
    result = resp.json().get("results")[0]
    assert result["Award ID"] == "IND12PB00323"
    assert result["Transaction Description"] == "test"
    assert result["Action Type"] == "A"
    assert result["Recipient UEI"] == "testuei"
    assert result["Recipient Location"] == {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "TX",
        "state_name": "Texas",
        "city_name": "cityname",
        "county_code": "001",
        "county_name": "testcountyname",
        "address_line1": "test address line",
        "address_line2": "address2",
        "address_line3": "address3",
        "congressional_code": "congressionalcode",
        "zip4": "6789",
        "zip5": "abcde",
        "foreign_postal_code": "foreignpostalcode",
        "foreign_province": "foreignprovince",
    }
    assert result["Primary Place of Performance"] == {
        "location_country_code": "popcountrycode",
        "country_name": "UNITED STATES",
        "state_code": "TX",
        "state_name": "Texas",
        "city_name": "popcityname",
        "county_code": "popcountycode",
        "county_name": "popcountyname",
        "congressional_code": "popcongressionalcode",
        "zip4": "popziplast4",
        "zip5": "popzip5",
    }

    assert result["NAICS"] == {"code": "naicscode", "description": "naicsdescription"}

    assert result["PSC"] == {"code": "psccode", "description": "pscdescription"}


def test_assistance_listing(client, monkeypatch, elasticsearch_transaction_index, transaction_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID", "Assistance Listing"]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["10"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    result = resp.json().get("results")[0]
    assert result["Assistance Listing"] == {"cfda_number": "59", "cfda_title": "cfdatitle"}
