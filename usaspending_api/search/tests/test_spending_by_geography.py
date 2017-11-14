import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search \
    import budget_function_data, all_filters


@pytest.fixture
def incorrect_location_data(db):
    country_usa = mommy.make(
        'references.RefCountryCode',
        country_code='USA'
    )

    location_wrong_state = mommy.make(
        'references.Location',
        location_id=10,
        location_country_code=country_usa,
        state_code='*'
    )

    location_wrong_county = mommy.make(
        'references.Location',
        location_id=11,
        location_country_code=country_usa,
        state_code='AL',
        county_code='9.0',
        county_name='COUNTY'

    )

    location_wrong_district = mommy.make(
        'references.Location',
        location_id=12,
        location_country_code=country_usa,
        state_code='AL',
        congressional_code='AL09',

    )

    transaction_state = mommy.make(
        'awards.TransactionNormalized',
        id=12345,
        place_of_performance=location_wrong_state,
        federal_action_obligation=50
    )

    transaction_county = mommy.make(
        'awards.TransactionNormalized',
        id=12346,
        place_of_performance=location_wrong_county,
        federal_action_obligation=100
    )

    transaction_county = mommy.make(
        'awards.TransactionNormalized',
        id=12347,
        place_of_performance=location_wrong_district,
        federal_action_obligation=0
    )


@pytest.mark.django_db
def test_spending_by_geography_success(client, budget_function_data):

    # test for required filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "place_of_performance",
            "geo_layer": "state",
            "filters": {
                'recipient_locations': [{'country': 'ABC'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "county",
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({'scope': 'test', 'filter': {}}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_geography_incorrect_state(client, incorrect_location_data):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'state',
            'filter': {}
        })
    )

    assert resp.data['results'][0]['display_name'] in ['Alabama', 'None']


@pytest.mark.django_db
def test_spending_by_geography_incorrect_county(client, incorrect_location_data):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'county',
            'filter': {}
        })
    )

    assert resp.data['results'][0]['display_name'] == 'County'


@pytest.mark.django_db
def test_spending_by_geography_incorrect_district(client, incorrect_location_data):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'district',
            'filter': {}
        })
    )

    assert len(resp.data['results']) == 0
