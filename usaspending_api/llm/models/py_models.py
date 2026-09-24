from typing import Annotated, Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, field_validator, model_validator

from usaspending_api.awards.v2.lookups.lookups import all_awards_types_to_category
from usaspending_api.common.helpers.orm_helpers import award_types_are_valid_groups


class InferenceConfig(BaseModel):
    """Model for AI inference configuration parameters.

    All fields accept None to allow the AI model to use its own defaults.
    When None, the parameter will be omitted from the inference request.
    """

    model_config = ConfigDict(extra="forbid")

    temperature: float | None = Field(
        default=0.0, ge=0.0, le=1.0, description="Temperature (0.0-1.0). Set to null to use model default."
    )
    topP: float | None = Field(
        default=1.0, ge=0.0, le=1.0, description="Top P sampling (0.0-1.0). Set to null to use model default."
    )
    maxTokens: int | None = Field(
        default=5000, gt=0, description="Maximum tokens to generate. Set to null to use model default."
    )
    stopSequences: list[str] | None = Field(
        default_factory=list,
        description="Stop sequences ([] for none, null for model default).",
    )


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
    state: str | None = Field(default=None, description="Two-letter state code (e.g., 'MO', 'TX', 'CA')")
    county: str | None = Field(default=None, description="Three-digit county code (e.g., '095')")
    city: str | None = Field(default=None, description="City name in uppercase (e.g., 'KANSAS CITY')")
    district_original: str | None = Field(
        default=None, description="The congressional district at the time of the contract date (e.g., '05')"
    )
    district_current: str | None = Field(default=None, description="The current congressional district (e.g., '04')")
    zip: str | None = Field(default=None, description="Five-digit zip code (e.g., '64198')")


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
    standalone: str = Field(description="Short location name for filter chips (e.g., 'Texas', 'Chicago', '64198')")
    title: str = Field(description="Full location name. Cities include state (e.g., 'KANSAS CITY, MISSOURI')")


class SelectedLocation(BaseModel):
    """Model for a selected location"""

    identifier: str = Field(
        description=(
            "Unique identifier using underscore-separated format: "
            "COUNTRY_STATE_DETAIL (e.g., 'USA_TX', 'USA_IL_CHICAGO', 'USA_64198')"
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
    subtier_agency: SubtierAgency | None = None
    agencyType: str = Field(alias="agencyType")


class CodeLists(BaseModel):
    """Base Model for code lists
    e.g. naics codes, psc codes, and tas codes
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


DEFCode = Literal[
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "AAA",
    "AAB",
    "AAC",
    "AAD",
    "AAE",
    "AAF",
    "AAG",
    "AAH",
    "AAI",
    "AAJ",
    "QQQ",
]

RecipientType = Literal[
    "business",
    "small_business",
    "other_than_small_business",
    "corporate_entity_tax_exempt",
    "corporate_entity_not_tax_exempt",
    "partnership_or_limited_liability_partnership",
    "sole_proprietorship",
    "manufacturer_of_goods",
    "subchapter_s_corporation",
    "limited_liability_corporation",
    "minority_owned_business",
    "alaskan_native_corporation_owned_firm",
    "american_indian_owned_business",
    "asian_pacific_american_owned_business",
    "black_american_owned_business",
    "hispanic_american_owned_business",
    "native_american_owned_business",
    "native_hawaiian_organization_owned_firm",
    "subcontinent_asian_indian_american_owned_business",
    "tribally_owned_firm",
    "other_minority_owned_business",
    "woman_owned_business",
    "women_owned_small_business",
    "economically_disadvantaged_women_owned_small_business",
    "joint_venture_women_owned_small_business",
    "joint_venture_economically_disadvantaged_women_owned_small_business",
    "veteran_owned_business",
    "service_disabled_veteran_owned_business",
    "special_designations",
    "8a_program_participant",
    "ability_one_program",
    "dot_certified_disadvantaged_business_enterprise",
    "emerging_small_business",
    "federally_funded_research_and_development_corp",
    "historically_underutilized_business_firm",
    "labor_surplus_area_firm",
    "sba_certified_8a_joint_venture",
    "self_certified_small_disadvanted_business",
    "small_agricultural_cooperative",
    "community_developed_corporation_owned_firm",
    "us_owned_business",
    "foreign_owned_and_us_located_business",
    "foreign_owned",
    "foreign_government",
    "international_organization",
    "domestic_shelter",
    "hospital",
    "veterinary_hospital",
    "nonprofit",
    "foundation",
    "community_development_corporations",
    "higher_education",
    "public_institution_of_higher_education",
    "private_institution_of_higher_education",
    "minority_serving_institution_of_higher_education",
    "school_of_forestry",
    "veterinary_college",
    "government",
    "national_government",
    "interstate_entity",
    "regional_and_state_government",
    "regional_organization",
    "us_territory_or_possession",
    "council_of_governments",
    "local_government",
    "indian_native_american_tribal_government",
    "authorities_and_commissions",
    "individuals",
]


class DEFCodeLists(BaseModel):
    """Validation model for DEFC code lists"""

    require: list[DEFCode] = Field(default_factory=list)
    exclude: list[DEFCode] = Field(default_factory=list)


# The frontend's predefined award-amount buckets and their fixed bounds (see
# https://github.com/fedspendingtransparency/usaspending-website/blob/master/src/js/dataMapping/search/awardAmount.js).
# None means the bound is open ended. Buckets may be combined with each other (OR semantics); a custom range instead
# uses the single 'specific' key.
AWARD_AMOUNT_RANGES: dict[str, list[int | None]] = {
    "range-0": [None, 1000000],
    "range-1": [1000000, 25000000],
    "range-2": [25000000, 100000000],
    "range-3": [100000000, 500000000],
    "range-4": [500000000, None],
}
AWARD_AMOUNT_KEYS = set(AWARD_AMOUNT_RANGES) | {"specific"}


class AwardAmounts(RootModel[dict[str, list[int | None]]]):
    """Validation model for the award-amount filter blob.

    The frontend stores award-amount filters as a dict of {key: [min, max]}, where a None bound is open
    ended. A key is either one or more of the predefined range buckets (range-0 .. range-4) — each of which
    carries fixed bounds — or the single 'specific' key holding a custom [min, max], which must stand alone.
    """

    @model_validator(mode="after")
    def validate_amounts(self) -> "AwardAmounts":
        amounts = self.root

        unknown = [key for key in amounts if key not in AWARD_AMOUNT_KEYS]
        if unknown:
            raise ValueError(f"Invalid award amount key(s): {unknown}. Valid keys are {sorted(AWARD_AMOUNT_KEYS)}.")

        if "specific" in amounts and len(amounts) > 1:
            raise ValueError("'specific' award amount must be the only key; it cannot be combined with range buckets.")

        for key, bounds in amounts.items():
            if key in AWARD_AMOUNT_RANGES:
                # Range buckets have fixed bounds; a custom range must use 'specific' instead.
                if bounds != AWARD_AMOUNT_RANGES[key]:
                    raise ValueError(
                        f"Award amount '{key}' must be {AWARD_AMOUNT_RANGES[key]}; use the 'specific' key "
                        f"for a custom range."
                    )
                continue

            # 'specific': a free-form [min, max] pair.
            if len(bounds) != 2:
                raise ValueError(f"Award amount '{key}' must be a [min, max] pair; got {bounds}.")
            lower, upper = bounds
            if lower is not None and lower < 0:
                raise ValueError(f"Award amount '{key}' min must be non-negative; got {lower}.")
            if upper is not None and upper < 0:
                raise ValueError(f"Award amount '{key}' max must be non-negative; got {upper}.")
            if lower is not None and upper is not None and upper < lower:
                raise ValueError(f"Award amount '{key}' max ({upper}) must be greater than or equal to min ({lower}).")

        return self


class Filters(BaseModel):
    """Validation model for all filter criteria and the persisted FilterHash blob."""

    model_config = ConfigDict(extra="forbid")

    keyword: list[str] = Field(default_factory=list)
    timePeriodType: Literal["fy", "dr"] = "fy"
    timePeriodFY: list[str] = []
    time_period: list[TimePeriod] = Field(default_factory=list)
    selectedLocations: dict[str, SelectedLocation] = Field(default_factory=dict)
    locationDomesticForeign: Literal["all", "foreign"] = "all"
    selectedFundingAgencies: dict[str, Any] = Field(default_factory=dict)
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: list[str] = Field(default_factory=list)
    recipientDomesticForeign: Literal["all", "foreign"] = "all"
    recipientType: list[RecipientType] = Field(default_factory=list)
    selectedRecipientLocations: dict[str, Any] = Field(default_factory=dict)
    awardType: list[str] = Field(default_factory=list)
    selectedAwardIDs: dict[str, Any] = Field(default_factory=dict)
    awardAmounts: AwardAmounts = Field(default_factory=lambda: AwardAmounts({}))
    selectedCFDA: dict[str, Any] = Field(default_factory=dict)
    naicsCodes: CodeLists = Field(default_factory=CodeLists)
    pscCodes: CodeLists = Field(default_factory=CodeLists)
    defCodes: DEFCodeLists = Field(default_factory=DEFCodeLists)
    pricingType: list[str] = Field(default_factory=list)
    setAside: list[str] = Field(default_factory=list)
    extentCompeted: list[str] = Field(default_factory=list)
    treasuryAccounts: dict[str, Any] = Field(default_factory=dict)
    tasCodes: CodeLists = Field(default_factory=CodeLists)
    awardDescription: str = ""
    filterNewAwardsOnlySelected: bool = False
    filterNewAwardsOnlyActive: bool = False
    filterNaoActiveFromFyOrDateRange: bool = False

    @field_validator("awardType")
    @classmethod
    def validate_award_type(cls, value: list[str]) -> list[str]:
        """Validate award-type codes: each must be a known code, and all must share one award group."""
        if not value:
            return value
        unknown = [code for code in value if code not in all_awards_types_to_category]
        if unknown:
            raise ValueError(f"Invalid award type code(s): {unknown}. Call list_award_type_codes for valid codes.")
        if not award_types_are_valid_groups(value):
            raise ValueError("'award_type_codes' must only contain types from one group.")
        return value

    @model_validator(mode="after")
    def validate_time_period_consistency(self) -> "Filters":
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


class DEFCodeListsWithoutEnum(BaseModel):
    """Version of DEFCodeLists with loose ``str`` codes instead of the DEFCode enum.
    This reduces the payload send to the llm with every call."""

    require: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="DEFC codes that must be present. Call list_defc_codes for valid codes.",
            json_schema_extra={"examples": [["L", "M", "N"]]},
        ),
    ]
    exclude: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="DEFC codes to exclude.",
            json_schema_extra={"examples": [["A"]]},
        ),
    ]


class ExecuteFilterInput(BaseModel):
    """Model for the input schema for the execute_filter tool

    This model is decoupled form the Filter model above.  the Filter model provides comprehensive validation.  This
    model is a lighter version that excludes the enumerated values in order to reduce the payload sent to the llm with
    every call.
    """

    model_config = ConfigDict(extra="forbid")

    keyword: list[str] = Field(
        default_factory=list,
        description="Free-text keywords. Only for terms with no matching structured filter.",
        json_schema_extra={"examples": [["bridge", "repair"]]},
    )
    timePeriodType: Literal["fy", "dr"] = Field(
        default="fy",
        description="Time period mode: 'fy' populates timePeriodFY; 'dr' populates time_period. Populate only one.",
    )
    timePeriodFY: Annotated[
        list[str],
        Field(
            description="Fiscal years as four-digit strings. Only when timePeriodType='fy'.",
            json_schema_extra={"examples": [["2023", "2024"]], "pattern": "^\\d{4}$"},
        ),
    ] = []
    time_period: Annotated[
        list[TimePeriod],
        Field(
            default_factory=list,
            description="Custom date ranges (YYYY-MM-DD). Only when timePeriodType='dr'.",
            json_schema_extra={"examples": [[{"start_date": "2023-01-01", "end_date": "2023-12-31"}]]},
        ),
    ]
    selectedLocations: Annotated[
        dict[str, SelectedLocation],
        Field(
            default_factory=dict,
            description=(
                "Selected locations keyed by identifier. Use the lookup_location tool to build these; "
                "do not construct them manually."
            ),
        ),
    ]
    locationDomesticForeign: Literal["all", "foreign"] = Field(
        default="all", description='Use "foreign" to search all foreign locations. Otherwise use "all".'
    )
    selectedFundingAgencies: dict[str, Any] = Field(
        default_factory=dict, description="Funding agencies keyed by id. Use the lookup_agency tool to build these."
    )
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(
        default_factory=dict, description="Awarding agencies keyed by id. Use the lookup_agency tool to build these."
    )
    selectedRecipients: list[str] = Field(
        default_factory=list, description="Recipient names. Use the lookup_recipient tool to resolve these."
    )
    recipientDomesticForeign: Literal["all", "foreign"] = Field(
        default="all", description='Use "foreign" to search all foreign recipient locations. Otherwise use "all".'
    )
    recipientType: list[str] = Field(
        default_factory=list,
        description=(
            "Business/organization type filter for award recipients (e.g. 'small_business'). "
            "Call list_recipient_types for all valid values grouped by category."
        ),
        json_schema_extra={"examples": [["small_business"], ["woman_owned_business", "minority_owned_business"]]},
    )
    selectedRecipientLocations: dict[str, Any] = Field(
        default_factory=dict,
        description="Recipient locations keyed by identifier. Use the lookup_location tool to build these.",
    )
    awardType: list[str] = Field(
        default_factory=list,
        description=(
            "Award-type code filter (e.g. contracts, grants, loans, IDVs). Call list_award_type_codes "
            "for all valid codes grouped by category. May only contain codes from a single group."
        ),
        json_schema_extra={"examples": [["02", "03", "04", "05"], ["A", "B", "C", "D"], ["07", "08"]]},
    )
    selectedAwardIDs: dict[str, Any] = Field(
        default_factory=dict, description="Award ID (PIID/FAIN/URI) filter keyed by identifier."
    )
    awardAmounts: dict[str, list[int | None]] = Field(
        default_factory=dict,
        description=(
            "Award amount ranges as {key: [min, max]}; None = unbounded. Predefined buckets have fixed "
            "bounds and are combinable: range-0 [,1M], range-1 [1M,25M], range-2 [25M,100M], "
            "range-3 [100M,500M], range-4 [500M,]. For a custom range use 'specific': [min, max], which "
            "must be the only key."
        ),
        json_schema_extra={
            "examples": [
                {"range-0": [None, 1000000], "range-2": [25000000, 100000000]},
                {"specific": [5000000, 50000000]},
            ]
        },
    )
    selectedCFDA: dict[str, Any] = Field(
        default_factory=dict,
        description="CFDA / Assistance Listing filter keyed by program number. Use the lookup_code tool.",
    )
    naicsCodes: CodeLists = Field(default_factory=CodeLists)
    pscCodes: CodeLists = Field(default_factory=CodeLists)
    defCodes: DEFCodeListsWithoutEnum = Field(
        default_factory=DEFCodeListsWithoutEnum,
        description=(
            "Disaster/Emergency Fund Codes (DEFC) filter with 'require'/'exclude' lists (e.g. COVID-19, "
            "Infrastructure, or Ukraine aid). Call list_defc_codes for all valid codes grouped by event."
        ),
    )
    pricingType: list[str] = Field(default_factory=list, description="Contract pricing type codes (e.g. 'A', 'B').")
    setAside: list[str] = Field(default_factory=list, description="Type-of-set-aside codes (e.g. 'SBA', 'SDVOSBC').")
    extentCompeted: list[str] = Field(default_factory=list, description="Extent-competed codes (e.g. 'A', 'D').")
    treasuryAccounts: dict[str, Any] = Field(
        default_factory=dict, description="Treasury Account Symbol (TAS) filter keyed by identifier."
    )
    tasCodes: CodeLists = Field(default_factory=CodeLists)
    awardDescription: str = Field(default="", description="Free-text award description search term.")
    filterNewAwardsOnlySelected: bool = Field(default=False, description="When true, limit results to new awards only.")
    filterNewAwardsOnlyActive: bool = Field(
        default=False, description="When true, the new-awards-only filter is active."
    )
    filterNaoActiveFromFyOrDateRange: bool = Field(
        default=False, description="When true, derive the new-awards-only window from the selected FY or date range."
    )


class FilterRequest(BaseModel):
    """Main model for the filter request"""

    filters: Filters
    version: str = "2020-06-01"


class FilterResponse(BaseModel):
    """Model for the API response"""

    hash: str
