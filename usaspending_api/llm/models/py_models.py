from typing import Annotated, Any, Callable, Literal

from pydantic import BaseModel, Field, model_validator, field_validator


class AIToolDescription(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]


class AITool(BaseModel):
    description: AIToolDescription
    function: Callable
    logging: Callable = lambda tool_use: print(f"Tool: {tool_use.name} with {tool_use.input}")


class LocationFilter(BaseModel):
    country: str = "USA"
    state: str | None = Field(default=None, description="Two-letter state code (e.g., 'TX', 'CA')")
    county: str | None = Field(default=None, description="Three-digit county code (e.g., '091')")
    city: str | None = Field(default=None, description="City name in uppercase (e.g., 'KANSAS CITY')")
    district_original: str | None = Field(
        default=None, description="The congressional district at the time of the contract date (e.g., '03')"
    )
    district_current: str | None = Field(default=None, description="The current congressional district (e.g., '03')")
    zip: str | None = Field(default=None, description="Five-digit zip code (e.g., '66208')")


class AwardAmountRange(BaseModel):
    """Model for award amount ranges"""

    pass


class LocationDisplay(BaseModel):
    """Model for location display information"""

    entity: Literal[
        "Country",
        "State",
        "County",
        "City",
        "Current congressional district",
        "Original congressional district",
        "Zip code",
    ] = Field(description="The type of geographic entity")
    standalone: str = Field(description="Short location name for filter chips (e.g., 'Texas', 'Chicago', '66208')")
    title: str = Field(description="Full location name. Cities include state (e.g., 'KANSAS CITY, MISSOURI')")


class SelectedLocation(BaseModel):
    """Model for a selected location"""

    identifier: str = Field(
        description=(
            "Unique identifier using underscore-separated format: "
            "COUNTRY_STATE_DETAIL (e.g., 'USA_TX', 'USA_IL_CHICAGO', 'USA_66208')"
        )
    )
    filter: LocationFilter
    display: LocationDisplay


class TimePeriod(BaseModel):
    """Time period with start and end dates"""

    start_date: Annotated[
        str,
        Field(
            description="Start date in YYYY-MM-DD format (e.g., '2023-01-15')",
            json_schema_extra={"pattern": "^\\d{4}-\\d{2}-\\d{2}$", "examples": ["2023-01-15", "2024-06-30"]},
        ),
    ]
    end_date: Annotated[
        str,
        Field(
            description="End date in YYYY-MM-DD format (e.g., '2024-12-31')",
            json_schema_extra={"pattern": "^\\d{4}-\\d{2}-\\d{2}$", "examples": ["2023-12-31", "2024-12-31"]},
        ),
    ]


class ToptierAgency(BaseModel):
    """Model for toptier agency information"""

    id: int
    toptier_code: str
    abbreviation: str
    name: str


class SubtierAgency(BaseModel):
    """Model for subtier agency information"""

    abbreviation: str
    name: str


class SelectedAgency(BaseModel):
    """Model for a selected agency"""

    id: int
    toptier_flag: bool
    toptier_agency: ToptierAgency
    subtier_agency: SubtierAgency
    agencyType: str = Field(alias="agencyType")


class CodeLists(BaseModel):
    """Model for code lists
    e.g. naics codes, def codes, psc codes, and tas codes
    """

    require: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="List of codes that must be present.",
            json_schema_extra={"examples": [["336411", "336412"]]},
        ),
    ]
    exclude: Annotated[
        list[str],
        Field(
            default_factory=list, description="List of codes to exclude", json_schema_extra={"examples": [["336413"]]}
        ),
    ]
    counts: list = Field(default_factory=list)


class Filters(BaseModel):
    """Model for all filter criteria"""

    keyword: list[str] = Field(
        default_factory=list, description="List of keywords. Use query fan out to expand user query to 2-3 synonyms"
    )
    timePeriodType: Annotated[
        Literal["fy", "dr"],
        Field(
            description=(
                "Time period type selector:\n"
                "- 'fy' (fiscal year): Use timePeriodFY field with year strings like ['2023', '2024']\n"
                "- 'dr' (date range): Use time_period field with TimePeriod objects containing start_date and end_date\n\n"
                "IMPORTANT: Only populate the field that matches this type."
            )
        ),
    ] = "fy"
    timePeriodFY: Annotated[
        list[str],
        Field(
            description=(
                "ONLY use when timePeriodType='fy'. "
                "List of fiscal years as four-digit strings (e.g., ['2023', '2024']). "
                "Leave empty if using date ranges (timePeriodType='dr')."
            ),
            json_schema_extra={"examples": [["2023", "2024", "2025"]], "pattern": "^\\d{4}$"},
        ),
    ] = []
    time_period: Annotated[
        list[TimePeriod],
        Field(
            default_factory=list,
            description=(
                "ONLY use when timePeriodType='dr'. "
                "List of custom date ranges with start_date and end_date in YYYY-MM-DD format. "
                "Leave empty if using fiscal years (timePeriodType='fy')."
            ),
            json_schema_extra={
                "examples": [
                    [
                        {"start_date": "2019-07-01", "end_date": "2021-06-30"},
                        {"start_date": "2022-01-01", "end_date": "2022-12-31"},
                    ]
                ]
            },
        ),
    ]
    selectedLocations: Annotated[
        dict[str, SelectedLocation],
        Field(
            default_factory=dict,
            description=(
                "Dictionary of selected locations keyed by their identifier. "
                "The key MUST match the 'identifier' field in the SelectedLocation value. "
                "\n\n"
                "IMPORTANT: Use the lookup_location tool to get properly formatted location objects. "
                "Do not construct these manually.\n\n"
                "Structure patterns:\n"
                "- Country only: 'DEU' → {country: 'DEU'}\n"
                "- State: 'USA_KS' → {country: 'USA', state: 'KS'}\n"
                "- County: 'USA_KS_091' → {country: 'USA', state: 'KS', county: '091'}\n"
                "- City: 'USA_MO_KANSAS CITY' → {country: 'USA', state: 'MO', city: 'KANSAS CITY'}\n"
                "- District: 'USA_KS_03' → {country: 'USA', state: 'KS', district_current: '03'}\n"
                "- Zip: 'USA_66208' → {country: 'USA', zip: '66208'}\n"
                "- Foreign city: 'TUR_undefined_ISTANBUL' → {country: 'TUR', city: 'ISTANBUL'}"
            ),
            json_schema_extra={
                "examples": [
                    {
                        "USA_TX": {
                            "identifier": "USA_TX",
                            "filter": {"country": "USA", "state": "TX"},
                            "display": {"entity": "State", "standalone": "TEXAS", "title": "TEXAS"},
                        },
                        "USA_IL_CHICAGO": {
                            "identifier": "USA_IL_CHICAGO",
                            "filter": {"country": "USA", "state": "IL", "city": "CHICAGO"},
                            "display": {"entity": "City", "standalone": "CHICAGO", "title": "CHICAGO, ILLINOIS"},
                        },
                    }
                ]
            },
        ),
    ]
    locationDomesticForeign: str = "all"
    selectedFundingAgencies: dict[str, Any] = Field(default_factory=dict)
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: list[str] = Field(default_factory=list)
    recipientDomesticForeign: str = "all"
    recipientType: list[str] = Field(default_factory=list)
    selectedRecipientLocations: dict[str, Any] = Field(default_factory=dict)
    awardType: list[str] = Field(default_factory=list)
    selectedAwardIDs: dict[str, Any] = Field(default_factory=dict)
    awardAmounts: dict[str, list[int | None]] = Field(
        default_factory=dict,
        description=(
            "Dictionary of award amount ranges for filtering. "
            "Each value is a two-element list: [min_amount, max_amount]. "
            "Use `None` for unbounded ranges.\n\n"
            "TWO MUTUALLY EXCLUSIVE MODES:\n\n"
            "MODE 1 - STANDARD RANGES (can select multiple):\n"
            "- 'range-0': [None, 1000000] - Awards up to $1M\n"
            "- 'range-1': [1000000, 25000000] - Awards $1M to $25M\n"
            "- 'range-2': [25000000, 100000000] - Awards $25M to $100M\n"
            "- 'range-3': [100000000, 500000000] - Awards $100M to $500M\n"
            "- 'range-4': [500000000, None] - Awards over $500M\n\n"
            "MODE 2 - SPECIFIC RANGE (must be alone):\n"
            "- 'specific': [min, max] - Specify exact dollar amounts\n\n"
            "CRITICAL RULES:\n"
            "1. You can use multiple standard ranges together (range-0 through range-4)\n"
            "2. You can use ONE specific range with specific min/max values\n"
            "3. NEVER mix standard ranges with specific range\n"
            "4. When using 'specific', it must be the ONLY key in the dictionary"
        ),
        json_schema_extra={
            "examples": [
                # Example 1: Multiple standard ranges
                {"range-0": [None, 1000000], "range-2": [25000000, 100000000]},
                # Example 2: Single standard range
                {"range-3": [100000000, 500000000]},
                # Example 3: Custom range with both bounds
                {"specific": [5000000, 50000000]},
                # Example 4: Custom range unbounded above
                {"specific": [10000000, None]},
                # Example 5: Custom range unbounded below
                {"specific": [None, 75000000]},
            ]
        },
    )
    selectedCFDA: dict[str, Any] = Field(default_factory=dict)
    naicsCodes: CodeLists = Field(default_factory=CodeLists)
    pscCodes: CodeLists = Field(default_factory=CodeLists)
    defCodes: CodeLists = Field(default_factory=CodeLists)
    defCode: list = Field(default_factory=list)
    pricingType: list[str] = Field(default_factory=list)
    setAside: list[str] = Field(default_factory=list)
    extentCompeted: list[str] = Field(default_factory=list)
    treasuryAccounts: dict[str, Any] = Field(default_factory=dict)
    tasCodes: CodeLists = Field(default_factory=CodeLists)
    awardDescription: str = ""
    searchedFilterValues: dict[str, Any] = Field(default_factory=dict)
    filterNewAwardsOnlySelected: bool = False
    filterNewAwardsOnlyActive: bool = False
    filterNaoActiveFromFyOrDateRange: bool = False

    @model_validator(mode="after")
    def validate_time_period_consistency(self):
        """Ensure only the correct time period field is populated"""
        if self.timePeriodType == "fy":
            if self.time_period:
                raise ValueError(
                    "When timePeriodType='fy', use timePeriodFY (list of years), not time_period (date ranges)"
                )

        elif self.timePeriodType == "dr":
            if self.timePeriodFY:
                raise ValueError(
                    "When timePeriodType='dr', use time_period (date ranges), not timePeriodFY (fiscal years)"
                )

        return self


class FilterRequest(BaseModel):
    """Main model for the filter request"""

    filters: Filters
    version: str = "2020-06-01"


class FilterResponse(BaseModel):
    """Model for the API response"""

    hash: str
