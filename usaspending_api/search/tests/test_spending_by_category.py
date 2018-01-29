import json

import pytest
from rest_framework import status


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_category_success(client):

    # test for required functions
    resp = client.post(
        '/api/v2/search/spending_by_category',
        content_type='application/json',
        data=json.dumps({
            "category": "funding_agency",
            "scope": "agency",
            "filters": {
                "keyword": "test"
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

    # TODO: Fix all filters test
    resp = client.post(
        '/api/v2/visualizations/spending_by_category',
        content_type='application/json',
        data=json.dumps({
            "group": "quarter",
            "filters": all_filters
        }))
    assert resp.status_code == status.HTTP_200_OK
    # test for similar matches (with no duplicates)


@pytest.mark.skip
@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_category/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
