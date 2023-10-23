import pytest
from model_bakery import baker

from usaspending_api.awards.v2.filters.location_filter_geocode import (
    geocode_filter_locations,
)
from usaspending_api.search.models import AwardSearch


@pytest.fixture
def award_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="BURBANK",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
        piid="piiiiid",
        pop_city_name="AUSTIN",
        pop_state_code="TX",
        pop_country_code="USA",
    )
    baker.make(
        "search.AwardSearch",
        award_id=1,
        is_fpds=True,
        latest_transaction_id=1,
        piid="piiiiid",
        type="A",
        recipient_location_city_name="BURBANK",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
        pop_city_name="AUSTIN",
        pop_state_code="TX",
        pop_country_code="USA",
    )

    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=2,
        award_id=2,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="BRISTOL",
        recipient_location_country_code="GBR",
        piid="piiiiid",
        pop_city_name="MCCOOL JUNCTION",
        pop_state_code="TX",
        pop_country_code="USA",
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        is_fpds=True,
        latest_transaction_id=2,
        piid="0001",
        type="A",
        recipient_location_city_name="BRISTOL",
        recipient_location_country_code="GBR",
        pop_city_name="MCCOOL JUNCTION",
        pop_state_code="TX",
        pop_country_code="USA",
    )

    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=3,
        award_id=3,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="BRISBANE",
        piid="0002",
        pop_city_name="BRISBANE",
        pop_state_code="NE",
        pop_country_code="USA",
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        is_fpds=True,
        latest_transaction_id=3,
        piid="0002",
        type="A",
        recipient_location_city_name="BRISBANE",
        pop_city_name="BRISBANE",
        pop_state_code="NE",
        pop_country_code="USA",
    )

    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=4,
        award_id=4,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="NEW YORK",
        recipient_location_country_code="USA",
        piid="0003",
        pop_city_name="NEW YORK",
        pop_state_code="NE",
        pop_country_code="USA",
    )
    baker.make(
        "search.AwardSearch",
        award_id=4,
        is_fpds=True,
        latest_transaction_id=4,
        piid="0003",
        type="A",
        recipient_location_city_name="NEW YORK",
        recipient_location_country_code="USA",
        pop_city_name="NEW YORK",
        pop_state_code="NE",
        pop_country_code="USA",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=5,
        award_id=5,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="NEW AMSTERDAM",
        recipient_location_country_code="USA",
        piid="0004",
        pop_city_name="NEW AMSTERDAM",
        pop_state_code="NE",
        pop_country_code="USA",
    )
    baker.make(
        "search.AwardSearch",
        award_id=5,
        is_fpds=True,
        latest_transaction_id=5,
        piid="0004",
        type="A",
        recipient_location_city_name="NEW AMSTERDAM",
        recipient_location_country_code="USA",
        pop_city_name="NEW AMSTERDAM",
        pop_state_code="NE",
        pop_country_code="USA",
    )

    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


@pytest.mark.django_db
def test_geocode_filter_locations(award_data_fixture):

    to = AwardSearch.objects

    values = [
        {"city": "McCool Junction", "state": "TX", "country": "USA"},
        {"city": "Burbank", "state": "CA", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("nothing", [])).count() == 5
    assert to.filter(geocode_filter_locations("pop", values)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values)).count() == 1

    values = [
        {"city": "Houston", "state": "TX", "country": "USA"},
        {"city": "McCool Junction", "state": "TX", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("pop", values)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values)).count() == 0
