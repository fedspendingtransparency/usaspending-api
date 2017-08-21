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
def test_spending_over_time_success(client, budget_function_data):

    # test for NAICS_description exact match
    resp = client.post(
        '/api/v2/visualizations/spending_over_time',
        content_type='application/json',
        data=json.dumps({
            "group": "fiscal_year",
            "filters": {
                "keyword": "test"
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    print(resp.data)
    all_filters = {
        "keyword": "test",
        "time_period": "2222-2-22",
        'award_type_codes': ['011','020'],
        'agencies': 'idk',
        'legal_entities': 'idk',
        'recipient_location_scope': 'idk',
        'recipient_locations': 'idk',
        'recipient_type_names': 'idk',
        'place_of_performance_scope': 'idk',
        'place_of_performances': 'idk',
        'award_amounts': 'idk',
        'award_ids': 'idk',
        'program_numbers': 'idk',
        'naics_codes': ['2','1'],
        'psc_codes': ["1","2"],
        'contract_pricing_type_codes': ["",""],
        'set_aside_type_codes': ["",""],
        'extent_competed_type_codes': [""]
    }
    resp = client.post(
        '/api/v2/visualizations/spending_over_time',
        content_type='application/json',
        data=json.dumps({
            "group": "quarter",
            "filters": all_filters
        }))
    # test for similar matches (with no duplicates)


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/visualizations/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
