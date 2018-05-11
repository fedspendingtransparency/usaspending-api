# Stdlib imports
import datetime

# Core Django imports

# Third-party app imports
from rest_framework import status
from model_mommy import mommy
import pytest



# Imports from your apps
from usaspending_api.common.helpers import generate_fiscal_year


def state_metadata_endpoint(fips, year=None):
    url = '/api/v2/recipient/state/{}/'.format(fips)
    if year:
        url = '{}?year={}'.format(url, year)
    return url


@pytest.fixture
def state_data(db):
    location_ts = mommy.make(
        'references.Location',
        location_country_code='USA',
        state_code='TS'
    )
    location_td = mommy.make(
        'references.Location',
        location_country_code='USA',
        state_code='TD'
    )
    location_tt = mommy.make(
        'references.Location',
        location_country_code='USA',
        state_code='TT'
    )
    mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_ts,
        federal_action_obligation=100000,
        action_date='2016-01-01'
    )
    mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_ts,
        federal_action_obligation=100000,
        action_date='2016-01-01'
    )
    mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_td,
        federal_action_obligation=1000,
        action_date='2016-01-01'
    )
    mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_tt,
        federal_action_obligation=1000,
        action_date='2016-01-01'
    )
    mommy.make(
        'recipient.StateData',
        id='01-2016',
        fips='01',
        name='Test State',
        code='TS',
        type='state',
        year=2016,
        population=50000,
        pop_source='Census 2010 Pop',
        median_household_income=50000,
        mhi_source='Census 2010 MHI'
    )
    mommy.make(
        'recipient.StateData',
        id='01-2017',
        fips='01',
        name='Test State',
        code='TS',
        type='state',
        year=2017,
        population=100000,
        pop_source='Census 2010 Pop',
        median_household_income=None,
        mhi_source='Census 2010 MHI'
    )
    mommy.make(
        'recipient.StateData',
        id='02-2016',
        fips='02',
        name='Test District',
        code='TD',
        type='district',
        year=2016,
        population=5000,
        pop_source='Census 2010 Pop',
        median_household_income=20000,
        mhi_source='Census 2010 MHI'
    )
    mommy.make(
        'recipient.StateData',
        id='03-2016',
        fips='03',
        name='Test Territory',
        code='TT',
        type='territory',
        year=2016,
        population=5000,
        pop_source='Census 2010 Pop',
        median_household_income=10000,
        mhi_source='Census 2010 MHI'
    )


@pytest.mark.django_db
def test_state_metadata_success(client, state_data):
    # test small request - state
    resp = client.get(state_metadata_endpoint('01'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['name'] == 'Test State'
    assert resp.data['code'] == 'TS'
    assert resp.data['fips'] == '01'
    assert resp.data['type'] == 'state'
    assert resp.data['population'] == 100000
    assert resp.data['pop_year'] == 2017
    assert resp.data['pop_source'] == 'Census 2010 Pop'
    assert resp.data['median_household_income'] == 50000
    assert resp.data['mhi_year'] == 2016
    assert resp.data['mhi_source'] == 'Census 2010 MHI'

    # test small request - district, testing 1 digit FIPS
    resp = client.get(state_metadata_endpoint('2'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['name'] == 'Test District'
    assert resp.data['code'] == 'TD'
    assert resp.data['type'] == 'district'

    # test small request - territory
    resp = client.get(state_metadata_endpoint('03'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['name'] == 'Test Territory'
    assert resp.data['code'] == 'TT'
    assert resp.data['type'] == 'territory'

    # test year with amounts
    resp = client.get(state_metadata_endpoint('01', '2016'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['population'] == 50000
    assert resp.data['pop_year'] == 2016
    assert resp.data['median_household_income'] == 50000
    assert resp.data['mhi_year'] == 2016
    assert resp.data['total_prime_amount'] == 200000
    assert resp.data['total_prime_awards'] == 2
    assert resp.data['award_amount_per_capita'] == 4

    # test future year
    resp = client.get(state_metadata_endpoint('01', '3000'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['pop_year'] == 2017
    assert resp.data['mhi_year'] == 2016

    # test old year
    resp = client.get(state_metadata_endpoint('01', '2000'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['pop_year'] == 2016
    assert resp.data['mhi_year'] == 2016

    # test latest year
    # Note: the state data will be based on 2016/2017 state metadata but amounts should be based on the last 12 months
    # Amounts aren't really testable as they're tied to the day its run
    resp = client.get(state_metadata_endpoint('01', 'latest'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['pop_year'] == 2017
    assert resp.data['mhi_year'] == 2016

    # test all years
    # Note: the state data will be based on 2016/2017 state metadata but amounts should be based on all years
    # Amounts aren't really testable as they're tied to the day its run
    resp = client.get(state_metadata_endpoint('01', 'all'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['pop_year'] == 2017
    assert resp.data['mhi_year'] == 2016
    assert resp.data['award_amount_per_capita'] is None

    # making sure amount per capita is null for current fiscal year
    resp = client.get(state_metadata_endpoint('01', generate_fiscal_year(datetime.date.today())))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['award_amount_per_capita'] is None


@pytest.mark.django_db
def test_state_metadata_failure(client, state_data):
    """Verify error on bad autocomplete request for budget function."""

    # There is no FIPS with 03
    resp = client.get(state_metadata_endpoint('04'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # There is no FIPS with 03
    resp = client.get(state_metadata_endpoint('01', 'break'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
