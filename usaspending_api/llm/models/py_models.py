from typing import Any, Callable, Dict, List

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
    country: str
    state: str | None = Field(default=None)
    county: str | None = Field(default=None)
    city: str | None = Field(default=None)
    district_original: str | None = Field(
        default=None, description="The congressional district at the time of the contract date."
    )
    district_current: str | None = Field(default=None, description="The current congressional district.")
    zip_code: str | None = Field(default=None)


class AdvancedSearchFilter(BaseModel):
    keywords: list[str] | None = Field(default=None)
    place_of_performance_locations: list[LocationFilter] | None = Field(default=None)


class AwardAmountRange(BaseModel):
    """Model for award amount ranges"""

    pass  # Dynamic keys like "range-1" with list values


class LocationDisplay(BaseModel):
    """Model for location display information"""

    entity: str
    standalone: str
    title: str


class SelectedLocation(BaseModel):
    """Model for a selected location"""

    identifier: str
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


class Filters(BaseModel):
    """Model for all filter criteria"""

    keyword: Dict[str, Any] = Field(default_factory=dict)
    timePeriodType: str = "fy"
    timePeriodFY: List[str] = Field(default_factory=list)
    timePeriodStart: str | None = None
    timePeriodEnd: str | None = None
    selectedLocations: Dict[str, SelectedLocation] = Field(default_factory=dict)
    locationDomesticForeign: str = "all"
    selectedFundingAgencies: Dict[str, Any] = Field(default_factory=dict)
    selectedAwardingAgencies: Dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: List[str] = Field(default_factory=list)
    recipientDomesticForeign: str = "all"
    recipientType: List[str] = Field(default_factory=list)
    selectedRecipientLocations: Dict[str, Any] = Field(default_factory=dict)
    awardType: List[str] = Field(default_factory=list)
    selectedAwardIDs: Dict[str, Any] = Field(default_factory=dict)
    awardAmounts: Dict[str, List[int]] = Field(default_factory=dict)
    selectedCFDA: Dict[str, Any] = Field(default_factory=dict)
    selectedNAICS: Dict[str, Any] = Field(default_factory=dict)
    selectedPSC: Dict[str, Any] = Field(default_factory=dict)
    pricingType: List[str] = Field(default_factory=list)
    setAside: List[str] = Field(default_factory=list)
    extentCompeted: List[str] = Field(default_factory=list)
    federalAccounts: Dict[str, Any] = Field(default_factory=dict)
    treasuryAccounts: Dict[str, Any] = Field(default_factory=dict)


class FilterRequest(BaseModel):
    """Main model for the filter request"""

    filters: Filters
    version: str | None = None


class FilterResponse(BaseModel):
    """Model for the API response"""

    hash: str
