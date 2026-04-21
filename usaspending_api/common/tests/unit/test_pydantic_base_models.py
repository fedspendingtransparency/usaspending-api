import pytest
from pydantic import ValidationError

from usaspending_api.common.pydantic_base_models import (
    AgencyObject,
    AwardAmount,
    NAICSCodeObject,
    ProgramActivityObject,
    PSCCodeObject,
    StandardLocationObject,
    TASCodeObject,
    TimePeriod,
    TreasuryAccountComponentsObject,
)


def test_code_objects_validation_pass():
    # NAICS
    naics = NAICSCodeObject(require=["one", "two"], exclude=["three", "four"])
    assert naics.require == ["one", "two"]
    assert naics.exclude == ["three", "four"]

    naics = NAICSCodeObject(require=["one", "two"])
    assert naics.require == ["one", "two"]
    assert naics.exclude is None

    # PSC
    psc = PSCCodeObject(require=[["one", "two"]], exclude=[["three", "four"]])
    assert psc.require == [["one", "two"]]
    assert psc.exclude == [["three", "four"]]

    psc = PSCCodeObject(exclude=[["three", "four"]])
    assert psc.require is None
    assert psc.exclude == [["three", "four"]]

    # TAS
    tas = TASCodeObject(require=[["one", "two"]], exclude=[["three", "four"]])
    assert tas.require == [["one", "two"]]
    assert tas.exclude == [["three", "four"]]

    tas = TASCodeObject()
    assert tas.require is None
    assert tas.exclude is None


def test_code_objects_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        PSCCodeObject(require=[[1, 2]])
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "require"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        TASCodeObject(exclude=[[True, False]])
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "exclude"
    assert errors[0]["msg"] == "Input should be a valid string"


def test_agency_validation_pass():
    agency = AgencyObject(type="awarding", tier="toptier", name="Fake Agency", toptier_name="Fake Agency Toptier")
    assert agency.type == "awarding"
    assert agency.tier == "toptier"
    assert agency.name == "Fake Agency"
    assert agency.toptier_name == "Fake Agency Toptier"

    agency = AgencyObject(type="awarding", tier="toptier", name="Fake Agency")
    assert agency.toptier_name is None


def test_agency_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        AgencyObject(type="bad_type")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "type"
    assert errors[0]["msg"] == "Input should be 'awarding' or 'funding'"

    with pytest.raises(ValidationError) as exc_info:
        AgencyObject(type="funding", tier="bad_tier")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "tier"
    assert errors[0]["msg"] == "Input should be 'toptier' or 'subtier'"

    with pytest.raises(ValidationError) as exc_info:
        AgencyObject(type="funding", tier="toptier", name=123)
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "name"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        AgencyObject(type="awarding", tier="subtier", name="Fake Agency", toptier_name=False)
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "toptier_name"
    assert errors[0]["msg"] == "Input should be a valid string"


def test_award_amount_validation_pass():
    amount = AwardAmount(lower_bound=1, upper_bound=10)
    assert amount.lower_bound == 1
    assert amount.upper_bound == 10

    # Validate that Pydantic can convert a string number into an int
    amount = AwardAmount(lower_bound="123", upper_bound=200)
    assert amount.lower_bound == 123
    assert amount.upper_bound == 200

    amount = AwardAmount()
    assert amount.lower_bound is None
    assert amount.upper_bound is None


def test_award_amount_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        AwardAmount(lower_bound=100, upper_bound=0)
    errors = exc_info.value.errors()
    assert errors[0]["msg"] == "Value error, upper_bound must be greater than or equal to lower_bound"

    with pytest.raises(ValidationError) as exc_info:
        AwardAmount(lower_bound="abc")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "lower_bound"
    assert errors[0]["msg"] == "Input should be a valid integer, unable to parse string as an integer"

    with pytest.raises(ValidationError) as exc_info:
        AwardAmount(upper_bound="xyz")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "upper_bound"
    assert errors[0]["msg"] == "Input should be a valid integer, unable to parse string as an integer"


def test_time_period_validation_pass():
    valid_date_types = ["action_date", "date_signed", "last_modified_date", "new_awards_only"]

    for date_type in valid_date_types:
        period = TimePeriod(start_date="2026-01-02", end_date="2026-01-03", date_type=date_type)
        assert period.start_date == "2026-01-02"
        assert period.end_date == "2026-01-03"
        assert period.date_type == date_type


def test_time_period_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date=123, end_date="2026-01-01", date_type="action_date")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "start_date"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date="2026-01-01", end_date=123, date_type="action_date")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "end_date"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date="2026-01-01", end_date="2026-01-05", date_type="bad")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "date_type"
    assert errors[0]["msg"] == "Input should be 'action_date', 'date_signed', 'last_modified_date' or 'new_awards_only'"

    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date="2006-01-01", end_date="2026-01-01")
    errors = exc_info.value.errors()
    assert errors[0]["msg"] == "Value error, start_date cannot be earlier than '2007-10-01'"

    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date="2026-01-01", end_date="2006-01-01")
    errors = exc_info.value.errors()
    assert errors[0]["msg"] == "Value error, end_date cannot be earlier than '2007-10-01'"

    with pytest.raises(ValidationError) as exc_info:
        TimePeriod(start_date="2026-01-01", end_date="06/01/2026", date_type="action_date")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "end_date"
    assert errors[0]["msg"] == "Value error, Date must be in YYYY-MM-DD format"


def test_treasury_account_components_validation_pass():
    tac = TreasuryAccountComponentsObject(aid="a", main="b", a="c", ata="d", bpoa="e", epoa="f", sub="g")
    assert tac.aid == "a"
    assert tac.main == "b"
    assert tac.a == "c"
    assert tac.ata == "d"
    assert tac.bpoa == "e"
    assert tac.epoa == "f"
    assert tac.sub == "g"

    tac = TreasuryAccountComponentsObject(aid="a", main="b")
    assert tac.aid == "a"
    assert tac.main == "b"
    assert all(x is None for x in [tac.a, tac.ata, tac.bpoa, tac.epoa, tac.sub])


def test_treasury_account_components_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        TreasuryAccountComponentsObject(aid=123, main="b")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "aid"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        TreasuryAccountComponentsObject(aid="a", main=123)
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "main"
    assert errors[0]["msg"] == "Input should be a valid string"


def test_program_activity_validation_pass():
    pa = ProgramActivityObject(name="Activity 1", code="abc-123-def")
    assert pa.name == "Activity 1"
    assert pa.code == "abc-123-def"

    pa = ProgramActivityObject(name="Activity 1")
    assert pa.name == "Activity 1"
    assert pa.code is None

    pa = ProgramActivityObject(code="abc-123-def")
    assert pa.name is None
    assert pa.code == "abc-123-def"


def test_program_activity_validation_fail():
    with pytest.raises(ValidationError) as exc_info:
        ProgramActivityObject(name=123, code="abc-123-def")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "name"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        ProgramActivityObject(name="Activity 1", code=123)
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "code"
    assert errors[0]["msg"] == "Input should be a valid string"

    with pytest.raises(ValidationError) as exc_info:
        ProgramActivityObject()
    errors = exc_info.value.errors()
    assert errors[0]["msg"] == "Value error, At least one of 'name' or 'code' must be provided"


def test_locations_validation_pass():
    location = StandardLocationObject(country="USA")
    assert location.country == "USA"

    location = StandardLocationObject(country="USA", state="CA")
    assert location.country == "USA"
    assert location.state == "CA"

    location = StandardLocationObject(country="USA", state="CA", county="001")
    assert location.country == "USA"
    assert location.state == "CA"
    assert location.county == "001"

    location = StandardLocationObject(country="USA", city="Los Angeles")
    assert location.country == "USA"
    assert location.state is None
    assert location.city == "Los Angeles"

    location = StandardLocationObject(country="USA", state="CA", city="Los Angeles",)
    assert location.country == "USA"
    assert location.state == "CA"
    assert location.city == "Los Angeles"

    location = StandardLocationObject(country="USA", state="CA", district_original="01")
    assert location.country == "USA"
    assert location.state == "CA"
    assert location.district_original == "01"

    location = StandardLocationObject(country="USA", state="CA", district_current="02")
    assert location.country == "USA"
    assert location.state == "CA"
    assert location.district_current == "02"

    location = StandardLocationObject(country="USA", zip="12345")
    assert location.country == "USA"
    assert location.zip == "12345"


def test_locations_basic_validation_fail():
    # Required fields
    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject()
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "country"
    assert errors[0]["msg"] == "Field required"

    # Field lengths
    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="United States of America")
    errors = exc_info.value.errors()
    assert errors[0]["loc"][0] == "country"
    assert errors[0]["msg"] == "Value error, Country code must be exactly 3 characters"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="California")
        errors = exc_info.value.errors()
        assert errors[0]["loc"][0] == "state"
        assert errors[0]["msg"] == "Value error, State code must be exactly 2 characters"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", county="Los Angeles County")
        errors = exc_info.value.errors()
        assert errors[0]["loc"][0] == "county"
        assert errors[0]["msg"] == "Value error, County code must be exactly 3 digits"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="CA", district_original="CA-01")
        errors = exc_info.value.errors()
        assert errors[0]["loc"][0] == "district_original"
        assert errors[0]["msg"] == "Value error, District code must be exactly 2 characters"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="CA", district_current="CA-01")
        errors = exc_info.value.errors()
        assert errors[0]["loc"][0] == "district_current"
        assert errors[0]["msg"] == "Value error, District code must be exactly 2 characters"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", zip="123")
        errors = exc_info.value.errors()
        assert errors[0]["loc"][0] == "zip"
        assert errors[0]["msg"] == "Value error, ZIP code must be exactly 5 digits"


def test_location_rules_validation():
    # Location rules (certain fields are required for each location level)
    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", county="001")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'county' is provided, 'state' must also be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="CA", county="001", district_original="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, 'county' and 'district_original' cannot both be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="CA", county="001", district_current="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, 'county' and 'district_current' cannot both be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state=None, district_original="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'district_original' is provided, 'state' must also be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="BAD", state="CA", district_original="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'district_original' is provided, 'country' must be 'USA'"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state="CA", district_original="01", district_current="02")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, 'district_original' and 'district_current' cannot both be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="USA", state=None, district_current="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'district_current' is provided, 'state' must also be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country="BAD", state="CA", district_current="01")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'district_current' is provided, 'country' must be 'USA'"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country=None, state=None, city="Los Angeles")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'city' is provided, either 'state' AND 'country' must be provided or 'country' must be provided"

    with pytest.raises(ValidationError) as exc_info:
        StandardLocationObject(country=None, state="CA", city="Los Angeles")
        errors = exc_info.value.errors()
        assert errors[0]["msg"] == "Value error, When 'city' is provided, either 'state' AND 'country' must be provided or 'country' must be provided"
