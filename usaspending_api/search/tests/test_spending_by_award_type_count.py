import json

import pytest
from rest_framework import status

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.tests.test_mock_data_search import all_filters

from model_mommy import mommy


@pytest.fixture
def award_data_fixture(db):
    mommy.make("awards.TransactionNormalized", id=2)
    mommy.make("references.LegalEntity", legal_entity_id=20)
    mommy.make(
        "awards.Award",
        recipient_id=20,
        base_and_all_options_value=None,
        base_exercised_options_val=None,
        category="loans",
        certified_date="2018-07-12",
        create_date="2018-02-03 18:54:06.983782+00",
        data_source="DBR",
        date_signed="2009-09-10",
        description="FORD MOTOR COMPANY",
        fain="DECF0000058",
        fiscal_year=2009,
        fpds_agency_id=None,
        fpds_parent_agency_id=None,
        funding_agency_id=None,
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        id=48518634,
        is_fpds=False,
        last_modified_date="2018-08-02",
        latest_transaction_id=2,
        non_federal_funding_amount=0,
        parent_award_piid=None,
        period_of_performance_current_end_date="2039-09-09",
        period_of_performance_start_date="2009-09-10",
        piid=None,
        potential_total_value_of_award=None,
        subaward_count=0,
        total_funding_amount=5907041570,
        total_loan_value=5907041570,
        total_obligation=5907041570,
        total_outlay=None,
        total_subaward_amount=None,
        total_subsidy_cost=3000186413,
        transaction_unique_id="A000_8900_DECF0000058_-NONE-",
        type="07",
        type_description="DIRECT LOAN (E)",
        uri=None,
    )


@pytest.mark.django_db
def test_spending_by_award_type_success(client, refresh_matviews):

    # test for filters
    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A", "B", "C"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp = client.post(
        "/api/v2/search/spending_by_award_count",
        content_type="application/json",
        data=json.dumps({"filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"test": {}, "filters": {}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_award_no_intersection(client, db, award_data_fixture, refresh_matviews):

    request = {"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": {"award_type_codes": ["07"]}}

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["loans"] == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {
        "contracts": 0,
        "idvs": 0,
        "grants": 0,
        "direct_payments": 0,
        "loans": 0,
        "other": 0,
    }, "Results returned, they should all be 0"


@pytest.mark.django_db
def test_spending_by_award_subawards_no_intersection(client, mock_matviews_qs):
    mock_model_1 = MockModel(
        award_ts_vector="",
        subaward_id=9999,
        award_type="grant",
        prime_award_type="02",
        award_id=90,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1])

    request = {
        "subawards": True,
        "fields": ["Sub-Award ID"],
        "sort": "Sub-Award ID",
        "filters": {"award_type_codes": ["02", "03", "04", "05"]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["subgrants"] == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {"subcontracts": 0, "subgrants": 0}, "Results returned, there should all be 0"
