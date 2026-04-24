from typing import Annotated, Any, Callable, Literal

from pydantic import BaseModel, Field


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
    state: str | None = Field(default=None)
    county: str | None = Field(default=None)
    city: str | None = Field(default=None)
    district_original: str | None = Field(
        default=None, description="The congressional district at the time of the contract date."
    )
    district_current: str | None = Field(default=None, description="The current congressional district.")
    zip: str | None = Field(default=None, description="The zip code.")


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
    ]
    standalone: str = Field(
        description="How the location appears in the filter chip of the frontend."
        " Should use standalone primary location name e.g. 'Texas', 'Chicago', etc."
    )
    title: str = Field(
        description="Long version of the location. Usually the same as the standalone except cities include state."
    )


class SelectedLocation(BaseModel):
    """Model for a selected location"""

    identifier: str = Field(
        description=(
            "A unique identifier. The format is an underscore separated string starting with the country code"
            "followed by the location elements.  Example 'USA_TX', or USA_IL_CHICAGO, etc."
        )
    )
    filter: LocationFilter
    display: LocationDisplay


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

    keyword: list[str] = Field(default_factory=dict)
    timePeriodType: str = "fy"
    timePeriodFY: Annotated[
        list[str],
        Field(
            description="List of fiscal years as four-digit strings (e.g., ['2023', '2024'])",
            json_schema_extra={"examples": [["2023", "2024", "2025"]], "pattern": "^\\d{4}$"},
        ),
    ] = []
    time_period: list = Field(default_factory=list)
    selectedLocations: dict[str, SelectedLocation] = Field(default_factory=dict)
    locationDomesticForeign: str = "all"
    selectedFundingAgencies: dict[str, Any] = Field(default_factory=dict)
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: list[str] = Field(default_factory=list)
    recipientDomesticForeign: str = "all"
    recipientType: list[str] = Field(default_factory=list)
    selectedRecipientLocations: dict[str, Any] = Field(default_factory=dict)
    awardType: list[str] = Field(default_factory=list)
    selectedAwardIDs: dict[str, Any] = Field(default_factory=dict)
    awardAmounts: dict[str, list[int]] = Field(default_factory=dict)
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


class FilterRequest(BaseModel):
    """Main model for the filter request"""

    filters: Filters
    version: str = "2020-06-01"


class FilterResponse(BaseModel):
    """Model for the API response"""

    hash: str
