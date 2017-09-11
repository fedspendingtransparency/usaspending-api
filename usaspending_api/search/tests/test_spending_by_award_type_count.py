import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award, Transaction
from usaspending_api.references.models import Location, Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def budget_function_data(db):

    loc1 = mommy.make(
        Location,
        location_id=1)

    ttagency1 = mommy.make(
        ToptierAgency,
        toptier_agency_id=1)
    stagency1 = mommy.make(
        SubtierAgency,
        subtier_agency_id=1,)

    agency1 = mommy.make(
        Agency,
        id=1,
        toptier_agency=ttagency1,
        subtier_agency=stagency1)

    award1 = mommy.make(
        Award,
        id=1,
        description="test",
        type="011",
        category="business",
        period_of_performance_start_date="1111-11-11",
        place_of_performance=loc1,
        awarding_agency=agency1,
        funding_agency=agency1,
        total_obligation=1000000.10)

    trans1 = mommy.make(
        Transaction,
        action_date="2222-2-22",
        id=1,
        award=award1,
        federal_action_obligation=50)


@pytest.mark.django_db
def test_spending_by_award_type_success(client, budget_function_data):

    # test for NAICS_description exact match
    resp = client.post(
        '/api/v2/search/spending_by_award_count/',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["A", "B", "C"]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    all_filters = {
        "keyword": "test",
        "time_period": [
            {
                "start_date": "2016-10-01",
                "end_date": "2017-09-30"
            }
        ],
        'award_type_codes': ['011', '020'],
        "agencies": [
            {
                "type": "funding",
                "tier": "toptier",
                "name": "Office of Pizza"
            },
            {
                "type": "awarding",
                "tier": "subtier",
                "name": "Personal Pizza"
            }
        ],
        "legal_entities": [1, 2, 3],
        'recipient_scope': "domestic",
        "recipient_locations": [1, 2, 3],
        "recipient_type_names": [
            "Small Business",
            "Alaskan Native Owned Business"],
        "place_of_performance_scope": "domestic",
        "place_of_performance_locations": [1, 2, 3],
        "award_type_codes": ["A", "B", "03"],
        "award_ids": [1, 2, 3],
        "award_amounts": [
            {
                "lower_bound": 1000000.00,
                "upper_bound": 25000000.00
            },
            {
                "upper_bound": 1000000.00
            },
            {
                "lower_bound": 500000000.00
            }
        ],
        "program_numbers": ["10.553"],
        "naics_codes": ["336411"],
        "psc_codes": ["1510"],
        "contract_pricing_type_codes": ["SAMPLECODE_CPTC"],
        "set_aside_type_codes": ["SAMPLECODE123"],
        "extent_competed_type_codes": ["SAMPLECODE_ECTC"]
    }
    resp = client.post(
        '/api/v2/search/spending_by_award_count',
        content_type='application/json',
        data=json.dumps({
            "filters": all_filters
        }))
    # test for similar matches (with no duplicates)


@pytest.mark.django_db
def test_spending_by_award_type_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_award_count/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
