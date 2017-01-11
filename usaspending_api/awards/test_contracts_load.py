import os

from django.conf import settings
from django.core.management import call_command
from django.test import TransactionTestCase, Client
from model_mommy import mommy
import pytest

from usaspending_api.awards.models import Award
from usaspending_api.awards.management.commands import loadcontracts
from usaspending_api.references.models import Location


# Transaction test cases so threads can find the data
class ContractsLoadTests(TransactionTestCase):

    fixtures = ['endpoint_fixture_db']

    @pytest.mark.django_db
    def test_contract_load(self):
        """
        Ensure contract awards can be loaded from usaspending
        """
        call_command('loadcontracts', os.path.join(settings.BASE_DIR, 'usaspending_api/data/usaspending_treasury_contracts.csv'))

    def teardown():
        Award.objects.all().delete()


@pytest.mark.parametrize('contract_type,expected', [
    ('a b c d', 'a'),   # splits and cares only about 1 char
    ('a:b:c d:e:f g', 'a'),
    ('something-BPA related', 'A'),
    ('something with pUrCHases', 'B'),
    ('delivery:aaa definitive:bbb', 'C'),   # check in order
    ('definitive', 'D'),
    ('something else entirely', None)
])
def test_evaluate_contract_award_type(contract_type, expected):
    """Verify that contract types are converted correctly"""
    row = dict(contractactiontype=contract_type)
    result = loadcontracts.evaluate_contract_award_type(row)
    assert result == expected


@pytest.mark.django_db
def test_get_or_create_location_ref_country_code():
    """Grab the location with this reference country code"""
    # dummy data
    mommy.make('references.RefCountryCode', _quantity=3, _fill_optional=True)
    mommy.make('references.Location', _quantity=3, _fill_optional=True)
    ref = mommy.make('references.RefCountryCode', country_code='USA',
                     _fill_optional=True)
    expected = mommy.make(
        'references.Location', location_country_code=ref,
        location_zip5='12345', location_zip_last4='6789',
        _fill_optional=True
    )

    row = dict(vendorcountrycode='USA', zipcode='12345-6789',
               streetaddress=expected.location_address_line1,
               streetaddress2=expected.location_address_line2,
               streetaddress3=expected.location_address_line3,
               state=expected.location_state_code,
               city=expected.location_city_name)
    assert loadcontracts.get_or_create_location(row) == expected


@pytest.mark.django_db
def test_get_or_create_location_ref_country_name():
    """Grab the location with this reference country name, accounting for
    capitialization differences"""
    # dummy data
    mommy.make('references.RefCountryCode', _quantity=3, _fill_optional=True)
    mommy.make('references.Location', _quantity=3, _fill_optional=True)
    ref = mommy.make('references.RefCountryCode', country_code='USA',
                     country_name='AmErIcA', _fill_optional=True)
    expected = mommy.make(
        'references.Location', location_country_code=ref,
        location_zip5='12345', location_zip_last4='6789',
        _fill_optional=True
    )

    row = dict(vendorcountrycode='america', zipcode='12345-6789',
               streetaddress=expected.location_address_line1,
               streetaddress2=expected.location_address_line2,
               streetaddress3=expected.location_address_line3,
               state=expected.location_state_code,
               city=expected.location_city_name)
    assert loadcontracts.get_or_create_location(row) == expected


@pytest.mark.django_db
def test_get_or_create_location_ref_country_name_complicated():
    """If we can't find a suitable reference country name, we try to re-order
    the words"""
    # dummy data
    mommy.make('references.RefCountryCode', _quantity=3, _fill_optional=True)
    mommy.make('references.Location', _quantity=3, _fill_optional=True)
    ref = mommy.make('references.RefCountryCode', country_code='USA',
                     country_name='aMerIca United States of (U.S.A.)',
                     _fill_optional=True)
    expected = mommy.make(
        'references.Location', location_country_code=ref,
        location_zip5='12345', location_zip_last4='6789',
        _fill_optional=True
    )

    row = dict(vendorcountrycode='america (u.s.a.)', zipcode='12345-6789',
               streetaddress=expected.location_address_line1,
               streetaddress2=expected.location_address_line2,
               streetaddress3=expected.location_address_line3,
               state=expected.location_state_code,
               city=expected.location_city_name)
    assert loadcontracts.get_or_create_location(row) == expected


@pytest.mark.django_db
def test_get_or_create_location_non_usa():
    """We should query different fields if it's a non-US row"""
    ref = mommy.make('references.RefCountryCode', country_code='UAE',
                     _fill_optional=True)
    expected = mommy.make(
        'references.Location', location_country_code=ref,
        location_zip5='12345', location_zip_last4='6789',
        # @todo: city_name has a different length than foreign_city_name, so
        # we can't use the random value
        location_city_name='AAAAAAAA',
        _fill_optional=True
    )

    row = dict(vendorcountrycode='UAE', zipcode='12345-6789',
               streetaddress=expected.location_address_line1,
               streetaddress2=expected.location_address_line2,
               streetaddress3=expected.location_address_line3,
               state=expected.location_state_code,
               city=expected.location_city_name)

    # can't find it because we're looking at the US fields
    assert loadcontracts.get_or_create_location(row) != expected

    row['zipcode'] = expected.location_foreign_postal_code
    row['state'] = expected.location_foreign_province
    row['city'] = expected.location_foreign_city_name
    assert loadcontracts.get_or_create_location(row) == expected


@pytest.mark.django_db
def test_get_or_create_location_creates_new_locations():
    """If no location is found, we create a new one"""
    ref = mommy.make('references.RefCountryCode', country_code='USA',
                     _fill_optional=True)
    row = dict(vendorcountrycode='USA', zipcode='12345-6789',
               streetaddress='Addy1', streetaddress2='Addy2',
               streetaddress3=None, state='ST', city='My Town')

    # can't find it because we're looking at the US fields
    assert Location.objects.count() == 0
    
    loadcontracts.get_or_create_location(row)
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
