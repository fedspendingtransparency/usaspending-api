from model_mommy import mommy
import pytest

from usaspending_api.common.api_request_utils import GeoCompleteHandler
from usaspending_api.references.models import Location, RefCityCountyCode


@pytest.mark.django_db
def test_location_reference_fill():
    city_county_code = mommy.make("references.RefCityCountyCode", city_code="A", county_code="B", _fill_optional=True)
    location = mommy.make("references.Location", location_country_code="USA", city_code="A", county_code="B")
    assert location.city_name == city_county_code.city_name
    assert location.county_name == city_county_code.county_name
    assert location.state_code == city_county_code.state_code


@pytest.mark.django_db
def test_location_state_fill():
    "Test populating missing state info"

    # Correct where one or the other is missing
    loc = mommy.make("references.Location", state_code="MN", country_name="UNITED STATES", _fill_optional=False)
    loc.save()
    assert loc.state_name == "MINNESOTA"

    loc = mommy.make("references.Location", state_name="MINNESOTA", country_name="UNITED STATES", _fill_optional=False)
    loc.save()
    assert loc.state_code == "MN"

    # Do not correct if both are full, even if it's wrong
    loc = mommy.make(
        "references.Location", state_name="OHIO", state_code="OK", country_name="UNITED STATES", _fill_optional=False
    )
    loc.save()
    assert loc.state_name == "OHIO"
    assert loc.state_code == "OK"

    # Do not correct if not clearly a US state
    loc = mommy.make("references.Location", state_name="OHAYOU", country_name="UNITED STATES", _fill_optional=False)
    loc.save()
    assert not loc.state_code

    # Do not correct if not US
    loc = mommy.make("references.Location", state_name="MINNESOTA", country_name="CANADA", _fill_optional=False)
    loc.save()
    assert not loc.state_code


@pytest.mark.django_db
def test_geocomplete_scope():
    mommy.make("references.Location", location_country_code="USA", country_name="United States")
    mommy.make("references.Location", location_country_code="CAN", country_name="Canada")

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
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        state_name="Pennsylvania",
        state_code="PA",
        city_name="Altoona",
        county_name="Lakawana",
        _quantity=50,
    )

    response_2 = GeoCompleteHandler({"value": "a", "limit": 2}).build_response()
    response_4 = GeoCompleteHandler({"value": "a", "limit": 4}).build_response()
    response_5 = GeoCompleteHandler({"value": "a", "limit": 5}).build_response()

    assert len(response_2) == 2
    assert len(response_4) == 4
    assert len(response_5) == 5


@pytest.mark.django_db
def test_geocomplete_congressional_codes():
    mommy.make("references.Location", location_country_code="USA", congressional_code="00", state_code="VA")
    mommy.make("references.Location", location_country_code="USA", congressional_code="01", state_code="VA")
    mommy.make("references.Location", location_country_code="USA", congressional_code="02", state_code="VA")
    mommy.make("references.Location", location_country_code="USA", congressional_code="00", state_code="UT")

    response_va = GeoCompleteHandler({"value": "VA-"}).build_response()
    response_ut = GeoCompleteHandler({"value": "UT-"}).build_response()
    response_va00 = GeoCompleteHandler({"value": "VA-00"}).build_response()

    assert len(response_va) == 3
    assert len(response_ut) == 1
    assert len(response_va00) == 1


@pytest.mark.django_db
def test_geocomplete_usage_flag():
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        place_of_performance_flag=True,
        recipient_flag=False,
        state_code="VA",
    )
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        place_of_performance_flag=True,
        recipient_flag=False,
        state_code="AZ",
    )
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        place_of_performance_flag=False,
        recipient_flag=True,
        state_code="VA",
    )
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        place_of_performance_flag=False,
        recipient_flag=True,
        state_code="AZ",
    )
    mommy.make(
        "references.Location",
        location_country_code="USA",
        country_name="United States",
        place_of_performance_flag=False,
        recipient_flag=True,
        state_code="LA",
    )

    response_pop = GeoCompleteHandler({"value": "a", "usage": "place_of_performance"}).build_response()
    response_recipient = GeoCompleteHandler({"value": "a", "usage": "recipient"}).build_response()

    assert Location.objects.count() == 5
    assert Location.objects.filter(place_of_performance_flag=True).count() == 2
    assert Location.objects.filter(recipient_flag=True).count() == 3
    assert len(response_recipient) == 4
    assert len(response_pop) == 3


@pytest.mark.django_db
def test_canonicalize_city_county():
    mommy.make("references.RefCityCountyCode", _fill_optional=True, _quantity=3)
    RefCityCountyCode.canonicalize()
    for cccode in RefCityCountyCode.objects.all():
        assert cccode.city_name == cccode.city_name.upper().strip()
        assert cccode.county_name == cccode.county_name.upper().strip()
