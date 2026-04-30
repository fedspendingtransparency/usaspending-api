import pytest
from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    CodeLists,
    FilterRequest,
    FilterResponse,
    Filters,
    LocationDisplay,
    LocationFilter,
    SelectedAgency,
    SelectedLocation,
    SubtierAgency,
    TimePeriod,
    ToptierAgency,
)


class TestAIToolDescription:
    def test_create_ai_tool_description(self):
        """Test creating an AIToolDescription"""
        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object", "properties": {}}
        )

        assert tool_desc.name == "test_tool"
        assert tool_desc.description == "A test tool"
        assert tool_desc.input_schema == {"type": "object", "properties": {}}

    def test_ai_tool_description_required_fields(self):
        """Test that all fields are required"""
        with pytest.raises(ValidationError):
            AIToolDescription(name="test")


class TestAITool:
    def test_create_ai_tool(self):
        """Test creating an AITool"""
        def test_function():
            return "result"

        tool_desc = AIToolDescription(
            name="test_tool",
            description="A test tool",
            input_schema={}
        )

        tool = AITool(
            description=tool_desc,
            function=test_function
        )

        assert tool.description == tool_desc
        assert tool.function() == "result"
        assert callable(tool.logging)


class TestLocationFilter:
    def test_create_location_filter_defaults(self):
        """Test LocationFilter with default values"""
        loc_filter = LocationFilter()

        assert loc_filter.country == "USA"
        assert loc_filter.state is None
        assert loc_filter.county is None
        assert loc_filter.city is None
        assert loc_filter.district_original is None
        assert loc_filter.district_current is None
        assert loc_filter.zip is None

    def test_create_location_filter_state(self):
        """Test LocationFilter for a state"""
        loc_filter = LocationFilter(country="USA", state="TX")

        assert loc_filter.country == "USA"
        assert loc_filter.state == "TX"

    def test_create_location_filter_city(self):
        """Test LocationFilter for a city"""
        loc_filter = LocationFilter(
            country="USA",
            state="IL",
            city="CHICAGO"
        )

        assert loc_filter.country == "USA"
        assert loc_filter.state == "IL"
        assert loc_filter.city == "CHICAGO"

    def test_create_location_filter_zip(self):
        """Test LocationFilter for a zip code"""
        loc_filter = LocationFilter(country="USA", zip="66208")

        assert loc_filter.country == "USA"
        assert loc_filter.zip == "66208"


class TestLocationDisplay:
    def test_create_location_display(self):
        """Test creating a LocationDisplay"""
        display = LocationDisplay(
            entity="State",
            standalone="Texas",
            title="TEXAS"
        )

        assert display.entity == "State"
        assert display.standalone == "Texas"
        assert display.title == "TEXAS"

    def test_location_display_entity_literal(self):
        """Test that entity only accepts valid literals"""
        valid_entities = [
            "Country", "State", "County", "City",
            "Current congressional district",
            "Original congressional district",
            "Zip code"
        ]

        for entity in valid_entities:
            display = LocationDisplay(
                entity=entity,
                standalone="Test",
                title="TEST"
            )
            assert display.entity == entity

    def test_location_display_invalid_entity(self):
        """Test that invalid entity raises error"""
        with pytest.raises(ValidationError):
            LocationDisplay(
                entity="Invalid",
                standalone="Test",
                title="TEST"
            )


class TestSelectedLocation:
    def test_create_selected_location(self):
        """Test creating a complete SelectedLocation"""
        location = SelectedLocation(
            identifier="USA_TX",
            filter=LocationFilter(country="USA", state="TX"),
            display=LocationDisplay(
                entity="State",
                standalone="Texas",
                title="TEXAS"
            )
        )

        assert location.identifier == "USA_TX"
        assert location.filter.state == "TX"
        assert location.display.standalone == "Texas"


class TestTimePeriod:
    def test_create_time_period(self):
        """Test creating a TimePeriod"""
        period = TimePeriod(
            start_date="2023-01-01",
            end_date="2023-12-31"
        )

        assert period.start_date == "2023-01-01"
        assert period.end_date == "2023-12-31"

    def test_time_period_required_fields(self):
        """Test that both dates are required"""
        with pytest.raises(ValidationError):
            TimePeriod(start_date="2023-01-01")


class TestToptierAgency:
    def test_create_toptier_agency(self):
        """Test creating a ToptierAgency"""
        agency = ToptierAgency(
            id=1,
            toptier_code="075",
            abbreviation="HHS",
            name="Department of Health and Human Services"
        )

        assert agency.id == 1
        assert agency.toptier_code == "075"
        assert agency.abbreviation == "HHS"
        assert agency.name == "Department of Health and Human Services"


class TestSubtierAgency:
    def test_create_subtier_agency(self):
        """Test creating a SubtierAgency"""
        agency = SubtierAgency(
            abbreviation="CDC",
            name="Centers for Disease Control and Prevention"
        )

        assert agency.abbreviation == "CDC"
        assert agency.name == "Centers for Disease Control and Prevention"


class TestSelectedAgency:
    def test_create_selected_agency(self):
        """Test creating a SelectedAgency"""
        agency = SelectedAgency(
            id=1,
            toptier_flag=True,
            toptier_agency=ToptierAgency(
                id=1,
                toptier_code="075",
                abbreviation="HHS",
                name="Health and Human Services"
            ),
            subtier_agency=SubtierAgency(
                abbreviation="CDC",
                name="Centers for Disease Control"
            ),
            agencyType="awarding"
        )

        assert agency.id == 1
        assert agency.toptier_flag is True
        assert agency.toptier_agency.abbreviation == "HHS"
        assert agency.subtier_agency.abbreviation == "CDC"
        assert agency.agencyType == "awarding"


class TestCodeLists:
    def test_create_code_lists_defaults(self):
        """Test CodeLists with default values"""
        codes = CodeLists()

        assert codes.require == []
        assert codes.exclude == []
        assert codes.counts == []

    def test_create_code_lists_with_values(self):
        """Test CodeLists with values"""
        codes = CodeLists(
            require=["336411", "336412"],
            exclude=["336413"]
        )

        assert codes.require == ["336411", "336412"]
        assert codes.exclude == ["336413"]


class TestFilters:
    def test_create_filters_defaults(self):
        """Test Filters with default values"""
        filters = Filters()

        assert filters.keyword == []
        assert filters.timePeriodType == "fy"
        assert filters.timePeriodFY == []
        assert filters.time_period == []
        assert filters.selectedLocations == {}
        assert filters.locationDomesticForeign == "all"
        assert filters.awardType == []

    def test_create_filters_with_fiscal_years(self):
        """Test Filters with fiscal years"""
        filters = Filters(
            timePeriodType="fy",
            timePeriodFY=["2023", "2024"]
        )

        assert filters.timePeriodType == "fy"
        assert filters.timePeriodFY == ["2023", "2024"]
        assert filters.time_period == []

    def test_create_filters_with_date_range(self):
        """Test Filters with date range"""
        filters = Filters(
            timePeriodType="dr",
            time_period=[
                TimePeriod(start_date="2023-01-01", end_date="2023-12-31")
            ]
        )

        assert filters.timePeriodType == "dr"
        assert len(filters.time_period) == 1
        assert filters.timePeriodFY == []

    def test_filters_validation_fy_with_date_range_fails(self):
        """Test that using timePeriodFY with date ranges raises error"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(
                timePeriodType="fy",
                timePeriodFY=["2023"],
                time_period=[
                    TimePeriod(start_date="2023-01-01", end_date="2023-12-31")
                ]
            )

        assert "time_period" in str(exc_info.value)

    def test_filters_validation_dr_with_fiscal_years_fails(self):
        """Test that using date range with timePeriodFY raises error"""
        with pytest.raises(ValidationError) as exc_info:
            Filters(
                timePeriodType="dr",
                timePeriodFY=["2023"],
                time_period=[
                    TimePeriod(start_date="2023-01-01", end_date="2023-12-31")
                ]
            )

        assert "timePeriodFY" in str(exc_info.value)

    def test_filters_with_keywords(self):
        """Test Filters with keywords"""
        filters = Filters(
            keyword=["healthcare", "medical", "hospital"]
        )

        assert filters.keyword == ["healthcare", "medical", "hospital"]

    def test_filters_with_locations(self):
        """Test Filters with selected locations"""
        filters = Filters(
            selectedLocations={
                "USA_TX": SelectedLocation(
                    identifier="USA_TX",
                    filter=LocationFilter(country="USA", state="TX"),
                    display=LocationDisplay(
                        entity="State",
                        standalone="Texas",
                        title="TEXAS"
                    )
                )
            }
        )

        assert "USA_TX" in filters.selectedLocations
        assert filters.selectedLocations["USA_TX"].filter.state == "TX"

    def test_filters_with_naics_codes(self):
        """Test Filters with NAICS codes"""
        filters = Filters(
            naicsCodes=CodeLists(
                require=["336411", "336412"],
                exclude=["336413"]
            )
        )

        assert filters.naicsCodes.require == ["336411", "336412"]
        assert filters.naicsCodes.exclude == ["336413"]

    def test_filters_with_agencies(self):
        """Test Filters with agencies"""
        agency = SelectedAgency(
            id=1,
            toptier_flag=True,
            toptier_agency=ToptierAgency(
                id=1,
                toptier_code="075",
                abbreviation="HHS",
                name="Health and Human Services"
            ),
            subtier_agency=SubtierAgency(
                abbreviation="CDC",
                name="Centers for Disease Control"
            ),
            agencyType="awarding"
        )

        filters = Filters(
            selectedAwardingAgencies={"1": agency}
        )

        assert "1" in filters.selectedAwardingAgencies


class TestFilterRequest:
    def test_create_filter_request(self):
        """Test creating a FilterRequest"""
        request = FilterRequest(
            filters=Filters(keyword=["healthcare"]),
            version="2020-06-01"
        )

        assert request.version == "2020-06-01"
        assert request.filters.keyword == ["healthcare"]

    def test_filter_request_default_version(self):
        """Test FilterRequest default version"""
        request = FilterRequest(filters=Filters())

        assert request.version == "2020-06-01"

    def test_filter_request_serialization(self):
        """Test FilterRequest can be serialized to dict"""
        request = FilterRequest(
            filters=Filters(
                keyword=["test"],
                timePeriodType="fy",
                timePeriodFY=["2023"]
            )
        )

        data = request.model_dump(exclude_none=True)

        assert "filters" in data
        assert data["filters"]["keyword"] == ["test"]
        assert data["version"] == "2020-06-01"


class TestFilterResponse:
    def test_create_filter_response(self):
        """Test creating a FilterResponse"""
        response = FilterResponse(hash="abc123def456")

        assert response.hash == "abc123def456"

    def test_filter_response_required_hash(self):
        """Test that hash is required"""
        with pytest.raises(ValidationError):
            FilterResponse()


class TestComplexFilterScenarios:
    def test_complete_filter_with_all_fields(self):
        """Test creating a complex filter with many fields populated"""
        filters = Filters(
            keyword=["defense", "military", "army"],
            timePeriodType="fy",
            timePeriodFY=["2022", "2023", "2024"],
            selectedLocations={
                "USA_TX": SelectedLocation(
                    identifier="USA_TX",
                    filter=LocationFilter(country="USA", state="TX"),
                    display=LocationDisplay(
                        entity="State",
                        standalone="Texas",
                        title="TEXAS"
                    )
                ),
                "USA_CA": SelectedLocation(
                    identifier="USA_CA",
                    filter=LocationFilter(country="USA", state="CA"),
                    display=LocationDisplay(
                        entity="State",
                        standalone="California",
                        title="CALIFORNIA"
                    )
                )
            },
            awardType=["A", "B", "C", "D"],
            naicsCodes=CodeLists(
                require=["336411"],
                exclude=["336413"]
            ),
            pscCodes=CodeLists(
                require=["1510"]
            ),
            recipientType=["business"],
            setAside=["8AN", "SBA"],
            extentCompeted=["F", "A"]
        )

        assert len(filters.keyword) == 3
        assert len(filters.timePeriodFY) == 3
        assert len(filters.selectedLocations) == 2
        assert len(filters.awardType) == 4
        assert filters.naicsCodes.require == ["336411"]
        assert filters.pscCodes.require == ["1510"]

    def test_filter_json_schema_generation(self):
        """Test that Filters can generate JSON schema"""
        schema = Filters.model_json_schema()

        assert "properties" in schema
        assert "keyword" in schema["properties"]
        assert "timePeriodType" in schema["properties"]
        assert "selectedLocations" in schema["properties"]
