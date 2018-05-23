# Stdlib imports
import datetime

# Core Django imports

# Third-party app imports
from rest_framework import status
from model_mommy import mommy
import pytest

# Imports from your apps
from usaspending_api.common.helpers.generic_helper import generate_fiscal_year
from usaspending_api.recipient.v2.views.states import obtain_state_totals


EXPECTED_STATE = {
    'name': 'Test State',
    'code': 'TS',
    'fips': '01',
    'type': 'state',
    'population': 100000,
    'pop_year': 2017,
    'pop_source': 'Census 2010 Pop',
    'median_household_income': 50000,
    'mhi_year': 2016,
    'mhi_source': 'Census 2010 MHI',
    'total_prime_amount': 0,
    'total_prime_awards': 0,
    'award_amount_per_capita': 0.00
}
EXPECTED_DISTRICT = EXPECTED_STATE.copy()
EXPECTED_DISTRICT.update({
    'name': 'Test District',
    'code': 'TD',
    'type': 'district',
    'pop_year': 2016,
    'median_household_income': 20000,
    'fips': '02',
    'population': 5000
})
EXPECTED_TERRITORY = EXPECTED_STATE.copy()
EXPECTED_TERRITORY.update({
    'name': 'Test Territory',
    'code': 'TT',
    'type': 'territory',
    'pop_year': 2016,
    'median_household_income': 10000,
    'fips': '03',
    'population': 5000
})


def state_metadata_endpoint(fips, year=None):
    url = '/api/v2/recipient/state/{}/'.format(fips)
    if year:
        url = '{}?year={}'.format(url, year)
    return url


def sort_breakdown_response(response_list):
    """Sorting response since on Travis order of breakdown response list is different"""
    return sorted(response_list, key=lambda k: k['type'])


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


@pytest.fixture
def state_view_data(db, monkeypatch):
    monkeypatch.setattr('usaspending_api.recipient.v2.views.states.VALID_FIPS', {'01': {'code': 'AB'}})

    location = mommy.make(
        'references.Location',
        location_country_code='USA',
        state_code='AB'
    )

    award_2016 = mommy.make('awards.Award',
                            type='A')

    award_2017 = mommy.make('awards.Award',
                            type='B')

    trans_2016 = mommy.make(
        'awards.TransactionNormalized',
        award=award_2016,
        type='A',
        place_of_performance=location,
        federal_action_obligation=10,
        fiscal_year='2016',
        action_date='2016-01-01'
    )

    trans_2017 = mommy.make(
        'awards.TransactionNormalized',
        award=award_2017,
        type='B',
        place_of_performance=location,
        federal_action_obligation=15,
        fiscal_year='2017',
        action_date='2017-01-01'
    )

    mommy.make('awards.TransactionFPDS', transaction=trans_2016)
    mommy.make('awards.TransactionFPDS', transaction=trans_2017)


@pytest.fixture()
def state_breakdown_result():
    expected_result = [{'type': 'contracts', 'amount': 0, 'count': 0},
                       {'type': 'direct_payments', 'amount': 0, 'count': 0},
                       {'type': 'grants', 'amount': 0, 'count': 0},
                       {'type': 'loans', 'amount': 0, 'count': 0},
                       {'type': 'other_financial_assistance', 'amount': 0, 'count': 0}]

    return expected_result


@pytest.mark.django_db
def test_state_metadata_success(client, state_data, refresh_matviews):
    # test small request - state
    resp = client.get(state_metadata_endpoint('01'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == EXPECTED_STATE

    # test small request - territory
    resp = client.get(state_metadata_endpoint('03'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == EXPECTED_TERRITORY


@pytest.mark.django_db
def test_state_amounts_success(client, state_data, refresh_matviews):
    # test year with amounts
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'population': 50000,
        'pop_year': 2016,
        'median_household_income': 50000,
        'mhi_year': 2016,
        'total_prime_amount': 200000,
        'total_prime_awards': 2,
        'award_amount_per_capita': 4
    })
    resp = client.get(state_metadata_endpoint('01', '2016'))
    assert resp.data == expected_response
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_years_success(client, state_data, refresh_matviews):
    # test future year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'pop_year': 2017,
        'mhi_year': 2016
    })
    resp = client.get(state_metadata_endpoint('01', '3000'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response

    # test old year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'pop_year': 2016,
        'mhi_year': 2016,
        'population': 50000
    })
    resp = client.get(state_metadata_endpoint('01', '2000'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response

    # test latest year
    # Note: the state data will be based on 2016/2017 state metadata but amounts should be based on the last 12 months
    # Amounts aren't really testable as they're tied to the day its run
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'pop_year': 2017,
        'mhi_year': 2016
    })
    resp = client.get(state_metadata_endpoint('01', 'latest'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_current_all_years_success(client, state_data, refresh_matviews):
    # test all years
    # Note: the state data will be based on 2016/2017 state metadata but amounts should be based on all years
    # Amounts aren't really testable as they're tied to the day its run
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'pop_year': 2017,
        'mhi_year': 2016,
        'award_amount_per_capita': None,
        'total_prime_awards': 2,
        'total_prime_amount': 200000
    })
    resp = client.get(state_metadata_endpoint('01', 'all'))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_current_fy_capita_success(client, state_data, refresh_matviews):
    # making sure amount per capita is null for current fiscal year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({
        'award_amount_per_capita': None
    })
    resp = client.get(state_metadata_endpoint('01', generate_fiscal_year(datetime.date.today())))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_metadata_failure(client, state_data, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    # There is no FIPS with 03
    resp = client.get(state_metadata_endpoint('04'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # There is no FIPS with 03
    resp = client.get(state_metadata_endpoint('01', 'break'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # test small request - district, testing 1 digit FIPS
    resp = client.get(state_metadata_endpoint('2'))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_obtain_state_totals(state_view_data, refresh_matviews):
    result = obtain_state_totals('01', '2016', ['A'])
    expected = {'pop_state_code': 'AB', 'total': 10, 'count': 1}
    assert result == expected


@pytest.mark.django_db
def test_obtain_state_totals_none(state_view_data, refresh_matviews, monkeypatch):
    monkeypatch.setattr('usaspending_api.recipient.v2.views.states.VALID_FIPS', {'02': {'code': 'No State'}})
    result = obtain_state_totals('02')
    expected = {'pop_state_code': None, 'total': 0, 'count': 0}

    assert result == expected


@pytest.mark.django_db
def test_state_breakdown_success_state(client, state_view_data, state_breakdown_result, refresh_matviews):
    resp = client.get('/api/v2/recipient/state/awards/01/')
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result
    expected[0] = {'type': 'contracts', 'amount': 25, 'count': 2}

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_success_year(client, state_view_data, state_breakdown_result, refresh_matviews):
    resp = client.get('/api/v2/recipient/state/awards/01/?year=2017')
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result
    expected[0] = {'type': 'contracts', 'amount': 15, 'count': 1}

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_success_no_data(client, state_view_data, state_breakdown_result, refresh_matviews):
    resp = client.get('/api/v2/recipient/state/awards/01/?year=2015')
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_failure(client, state_view_data, refresh_matviews):
    resp = client.get('/api/v2/recipient/state/awards/05/')

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
