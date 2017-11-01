from model_mommy import mommy
import pytest

from usaspending_api.awards.v2.filters.award import award_filter


@pytest.fixture()
def award_data():
    country_1 = mommy.make(
        'references.RefCountryCode',
        country_code='USA'
    )

    country_2 = mommy.make(
        'references.RefCountryCode',
        country_code='GBR'
    )

    location_1 = mommy.make(
        'references.Location',
        location_country_code=country_1,
        state_code="BC", county_code='002',
        congressional_code='01'
    )

    location_2 = mommy.make(
        'references.Location',
        location_country_code=country_1,
        state_code="AB",
        county_code='001',
        congressional_code='01'
    )

    location_3 = mommy.make(
        'references.Location',
        location_country_code=country_1,
        state_code="AB",
        county_code='002',
        congressional_code='01'
    )

    location_4 = mommy.make(
        'references.Location',
        location_country_code=country_2,
        state_code=None,
        county_code=None,
        congressional_code=None
    )

    txn_1 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_1,
        recipient__location=location_1
    )

    txn_2 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_2,
        recipient__location=location_2
    )

    txn_3 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_3,
        recipient__location=location_3
    )

    txn_4 = mommy.make(
        'awards.TransactionNormalized',
        place_of_performance=location_4,
        recipient__location=location_4
    )

    award_1 = mommy.make(
        'awards.Award',
        place_of_performance=location_1,
        recipient__location=location_1,
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

    award_3 = mommy.make(
        'awards.Award',
        place_of_performance=location_3,
        recipient__location=location_3,
        latest_transaction=txn_3,
        category="A"
    )

    award_4 = mommy.make(
        'awards.Award',
        place_of_performance=location_4,
        recipient__location=location_4,
        latest_transaction=txn_4,
        category="A"
    )


@pytest.mark.django_db
def test_awards_filter_pop(award_data):

    # Testing state
    filter_state = {'place_of_performance_locations': [
        {'country': 'USA', 'state': 'BC'}
    ]}
    result = award_filter(filter_state)
    assert len(result) == 1

    # Testing county
    filter_county = {'place_of_performance_locations': [
        {'country': 'USA', 'state': 'AB', 'county': '002'}
    ]}
    result = award_filter(filter_county)
    assert len(result) == 1

    # Testing district
    filter_district = {'place_of_performance_locations': [
        {'country': 'USA', 'state': 'AB', 'district': '01'}
    ]}
    result = award_filter(filter_district)
    assert len(result) == 2

    # Testing country
    filter_country = {'place_of_performance_locations': [{'country': 'GBR'}]}
    result = award_filter(filter_country)
    assert len(result) == 1

    # Testing multiple
    filter_multiple = {'place_of_performance_locations': [
        {'country': 'USA', 'state': 'AB', 'district': '01'},
        {'country': 'GBR'}
    ]}
    result = award_filter(filter_multiple)
    assert len(result) == 3


@pytest.mark.django_db
def test_award_filter_recipient(award_data):
    # Testing state
    filter_state = {'recipient_locations': [{'country': 'USA', 'state': 'AB'}]}
    result = award_filter(filter_state)
    assert len(result) == 2

    # Testing county
    filter_county = {'recipient_locations': [
        {'country': 'USA', 'state': 'AB', 'county': '001'}
    ]}
    result = award_filter(filter_county)
    assert len(result) == 1

    # Testing district
    filter_district = {'recipient_locations': [
        {'country': 'USA', 'state': 'BC', 'district': '01'}
    ]}
    result = award_filter(filter_district)
    assert len(result) == 1

    # Testing country
    filter_country = {'recipient_locations': [{'country': 'USA'}]}
    result = award_filter(filter_country)
    assert len(result) == 3

    # Testing multiple
    filter_multiple = {'recipient_locations': [
        {'country': 'USA', 'state': 'AB', 'district': '01'},
        {'country': 'USA', 'state': 'AB', 'county': '001'}
    ]}
    result = award_filter(filter_multiple)
    assert len(result) == 2
