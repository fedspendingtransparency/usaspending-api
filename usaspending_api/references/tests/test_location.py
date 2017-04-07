import os

from django.conf import settings
from model_mommy import mommy
import pytest

from usaspending_api.common.api_request_utils import GeoCompleteHandler
from usaspending_api.references.models import Location


@pytest.mark.django_db
def test_location_reference_fill():
    country_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    city_county_code = mommy.make(
        'references.RefCityCountyCode', city_code="A", county_code="B", _fill_optional=True)
    location = mommy.make(
        'references.Location', location_country_code=country_code, city_code="A", county_code="B")
    assert location.city_name == city_county_code.city_name.upper()
    assert location.county_name == city_county_code.county_name.upper()
    assert location.state_code == city_county_code.state_code
    assert location.country_name == country_code.country_name


@pytest.mark.django_db
def test_geocomplete_scope():
    usa_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    can_code = mommy.make('references.RefCountryCode', country_code="CAN", country_name="Canada", _fill_optional=True)

    usa_location = mommy.make(
        'references.Location', location_country_code=usa_code)
    can_location = mommy.make(
        'references.Location', location_country_code=can_code)

    no_scope_response = GeoCompleteHandler({"value": "a"}).build_response()
    domestic_response = GeoCompleteHandler({"value": "a", "scope": "domestic"}).build_response()
    foreign_response = GeoCompleteHandler({"value": "a", "scope": "foreign"}).build_response()
    assert len(no_scope_response) == 2
    assert domestic_response[0]["place"] == "United States"
    assert len(domestic_response) == 1
    assert foreign_response[0]["place"] == "Canada"
    assert len(foreign_response) == 1


@pytest.mark.django_db
def test_geocomplete_limit():
    usa_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, _fill_optional=True, _quantity=50)

    response_10 = GeoCompleteHandler({"value": "a", "limit": 10}).build_response()
    response_25 = GeoCompleteHandler({"value": "a", "limit": 25}).build_response()
    response_50 = GeoCompleteHandler({"value": "a", "limit": 50}).build_response()

    assert len(response_10) == 10
    assert len(response_25) == 25
    assert len(response_50) == 50


@pytest.mark.django_db
def test_geocomplete_congressional_codes():
    usa_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, congressional_code="00", state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, congressional_code="01", state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, congressional_code="02", state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, congressional_code="00", state_code="UT", _fill_optional=True)

    response_va = GeoCompleteHandler({"value": "VA-"}).build_response()
    response_ut = GeoCompleteHandler({"value": "UT-"}).build_response()
    response_va00 = GeoCompleteHandler({"value": "VA-00"}).build_response()

    assert len(response_va) == 3
    assert len(response_ut) == 1
    assert len(response_va00) == 1


@pytest.mark.django_db
def test_geocomplete_usage_flag():
    usa_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    loc_va = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=True, recipient_flag=False, state_code="VA")
    loc_az = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=True, recipient_flag=False, state_code="AZ")
    loc2_va = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, state_code="VA")
    loc2_az = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, state_code="AZ")
    loc2_la = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, state_code="LA")

    response_pop = GeoCompleteHandler({"value": "a", "usage": "place_of_performance"}).build_response()
    response_recipient = GeoCompleteHandler({"value": "a", "usage": "recipient"}).build_response()

    assert Location.objects.count() == 5
    assert Location.objects.filter(place_of_performance_flag=True).count() == 2
    assert Location.objects.filter(recipient_flag=True).count() == 3
    assert len(response_recipient) == 4
    assert len(response_pop) == 3


@pytest.mark.django_db
def test_canonicalize():
    raw = {
        'state_code': 'oh',  # state_code not canonicalized
        'city_name': ' Dayton\n',
        'foreign_city_name': ' Däytön\n',
        'country_name': '\t\t\tusa',  # also not canonicalized
        'state_name': 'oHIo ',
        'county_name': ' montgomery ',
        'address_line1': '200   w 2nd\tST.',
        'address_line2': '#100',
        'address_line3': 'suite\n22',
        'foreign_province': ' ontariO',
        'foreign_city_name': ' Däytön\n',
    }
    loc = Location(**raw)
    loc.save()
    actual = set(loc.__dict__.items())
    desired = set({
        'address_line1': '200 W 2ND ST.',
        'address_line2': '#100',
        'address_line3': 'SUITE 22',
        'city_name': 'DAYTON',
        'country_name': '\t\t\tusa',
        'county_name': 'MONTGOMERY',
        'foreign_city_name': 'DÄYTÖN',
        'foreign_province': 'ONTARIO',
        'state_code': 'oh',
        'state_name': 'OHIO'
    }.items())
    assert not (desired - actual)
