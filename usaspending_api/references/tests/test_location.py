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
        'references.Location', location_country_code=country_code, location_city_code="A", location_county_code="B")
    assert location.location_city_name == city_county_code.city_name
    assert location.location_county_name == city_county_code.county_name
    assert location.location_state_code == city_county_code.state_code
    assert location.location_country_name == country_code.country_name


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
    locations = mommy.make('references.Location', location_country_code=usa_code, location_congressional_code="00", location_state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, location_congressional_code="01", location_state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, location_congressional_code="02", location_state_code="VA", _fill_optional=True)
    locations = mommy.make('references.Location', location_country_code=usa_code, location_congressional_code="00", location_state_code="UT", _fill_optional=True)

    response_va = GeoCompleteHandler({"value": "VA-"}).build_response()
    response_ut = GeoCompleteHandler({"value": "UT-"}).build_response()
    response_va00 = GeoCompleteHandler({"value": "VA-00"}).build_response()

    assert len(response_va) == 3
    assert len(response_ut) == 1
    assert len(response_va00) == 1


@pytest.mark.django_db
def test_geocomplete_usage_flag():
    usa_code = mommy.make('references.RefCountryCode', country_code="USA", country_name="United States", _fill_optional=True)
    loc_va = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=True, recipient_flag=False, location_state_code="VA")
    loc_az = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=True, recipient_flag=False, location_state_code="AZ")
    loc2_va = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, location_state_code="VA")
    loc2_az = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, location_state_code="AZ")
    loc2_la = mommy.make('references.Location', location_country_code=usa_code, place_of_performance_flag=False, recipient_flag=True, location_state_code="LA")

    response_pop = GeoCompleteHandler({"value": "a", "usage": "place_of_performance"}).build_response()
    response_recipient = GeoCompleteHandler({"value": "a", "usage": "recipient"}).build_response()

    assert Location.objects.count() == 5
    assert Location.objects.filter(place_of_performance_flag=True).count() == 2
    assert Location.objects.filter(recipient_flag=True).count() == 3
    assert len(response_recipient) == 4
    assert len(response_pop) == 3
