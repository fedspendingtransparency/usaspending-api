from model_mommy import mommy
import pytest

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter


@pytest.fixture()
def transaction_data():
    country_1 = mommy.make(
        'references.RefCountryCode',
        country_code='ABC'
    )

    location_1 = mommy.make(
        'references.Location',
        location_country_code=country_1,
        state_code="AA", county_code='001',
        congressional_code='01'
    )

    location_2 = mommy.make(
        'references.Location',
        location_country_code=country_1,
        state_code="AB",
        county_code='002',
        congressional_code='02'
    )

    txn_1 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_1,
        recipient__location=location_2
    )

    txn_2 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_2,
        recipient__location=location_2
    )

    award_1 = mommy.make(
        'awards.Award',
        place_of_performance=location_1,
        recipient__location=location_2,
        latest_transaction=txn_1,
        category="A"
    )

    award_2 = mommy.make(
        'awards.Award',
        place_of_performance=location_2,
        recipient__location=location_2,
        latest_transaction=txn_2,
        category="A"
    )


@pytest.mark.django_db
def test_transaction_filter_pop_country(transaction_data):
    # Testing country
    filter_country = {'place_of_performance_locations': [{'country': 'ABC'}]}
    result = transaction_filter(filter_country)
    assert len(result) == 2


@pytest.mark.django_db
def test_transaction_filter_recipient_state(transaction_data):
    # Testing state
    filter_state = {'recipient_locations': [{'country': 'ABC', 'state': 'AB'}]}
    result = transaction_filter(filter_state)
    assert len(result) == 2


@pytest.mark.django_db
def test_award_filter_pop_county(transaction_data):
    # Testing county
    filter_county = {'place_of_performance_locations': [
        {'country': 'ABC', 'state': 'AB', 'county': '002'}
    ]}
    result = award_filter(filter_county)
    assert len(result) == 1


@pytest.mark.django_db
def test_transaction_filter_pop_multi(transaction_data):
    # Testing multiple
    filter_multiple = {'place_of_performance_locations': [
        {'country': 'ABC', 'state': 'AA', 'district': '01'},
        {'country': 'ABC', 'state': 'AB', 'district': '02'},
    ]}
    result = transaction_filter(filter_multiple)
    assert len(result) == 2


@pytest.mark.django_db
def test_award_filter_recipient_error(transaction_data):
    # Testing for missing fields
    with pytest.raises(InvalidParameterException) as error_msg:
        filter_error = {'recipient_locations': [
            {'country': 'ABC', 'district': '01'},
            {'country': 'DEF'},
            {'country': 'ABC', 'state': 'AB', 'district': '02'}
        ]}
        award_filter(filter_error)
