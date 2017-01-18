import pytest
from model_mommy import mommy

from usaspending_api.awards.management.commands import helpers, loadcontracts
from usaspending_api.references.models import Location


@pytest.mark.django_db
def test_fetch_country_code():
    """Grab the location with this reference country code"""
    # dummy data
    mommy.make('references.RefCountryCode', _quantity=3, _fill_optional=True)
    code_match = mommy.make(
        'references.RefCountryCode', country_code='USA', _fill_optional=True)
    name_match = mommy.make(
        'references.RefCountryCode',
        country_name='AmErIcA',
        _fill_optional=True)
    complex_name = mommy.make(
        'references.RefCountryCode',
        country_name='aMerIca United States of (U.S.A.)',
        _fill_optional=True)
    assert helpers.fetch_country_code('USA') == code_match
    assert helpers.fetch_country_code('USA:more:stuff') == code_match
    assert helpers.fetch_country_code('america') == name_match
    assert helpers.fetch_country_code('aMEriCA: : : :') == name_match
    assert helpers.fetch_country_code('america (u.s.a.):stuff') == complex_name


@pytest.mark.django_db
def test_get_or_create_location_non_usa():
    """We should query different fields if it's a non-US row"""
    ref = mommy.make(
        'references.RefCountryCode', country_code='UAE', _fill_optional=True)
    expected = mommy.make(
        'references.Location',
        location_country_code=ref,
        location_zip5='12345',
        location_zip_last4='6789',
        # @todo: city_name has a different length than foreign_city_name, so
        # we can't use the random value
        location_city_name='AAAAAAAA',
        _fill_optional=True)

    row = dict(
        vendorcountrycode='UAE',
        zipcode='12345-6789',
        streetaddress=expected.location_address_line1,
        streetaddress2=expected.location_address_line2,
        streetaddress3=expected.location_address_line3,
        state=expected.location_state_code,
        city=expected.location_city_name)

    # can't find it because we're looking at the POP fields
    assert helpers.get_or_create_location(
        row, loadcontracts.location_mapper_place_of_performance) != expected


@pytest.mark.django_db
def test_get_or_create_location_creates_new_locations():
    """If no location is found, we create a new one"""
    ref = mommy.make(
        'references.RefCountryCode', country_code='USA', _fill_optional=True)
    row = dict(
        vendorcountrycode='USA',
        zipcode='12345-6789',
        streetaddress='Addy1',
        streetaddress2='Addy2',
        streetaddress3=None,
        vendor_state_code='ST',
        city='My Town')

    # can't find it because we're looking at the US fields
    assert Location.objects.count() == 0

    helpers.get_or_create_location(row, loadcontracts.location_mapper_vendor)
    assert Location.objects.count() == 1

    loc = Location.objects.all().first()
    assert loc.location_country_code == ref
    assert loc.location_zip5 == '12345'
    assert loc.location_zip_last4 == '6789'
    assert loc.location_address_line1 == 'Addy1'
    assert loc.location_address_line2 == 'Addy2'
    assert loc.location_address_line3 is None
    assert loc.location_state_code == 'ST'
    assert loc.location_city_name == 'My Town'
