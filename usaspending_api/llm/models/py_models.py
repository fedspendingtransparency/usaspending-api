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
    """Base Model for code lists"""

    require: Annotated[
        list[DEFCode],
        Field(
            default_factory=list,
            description="List of codes that must be present.",
            json_schema_extra={"examples": [["A", "AAB"]]},
        ),
    ]
    exclude: Annotated[
        list[DEFCode],
        Field(
            default_factory=list, description="List of codes to exclude", json_schema_extra={"examples": [["336413"]]}
        ),
    ]


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
    locationDomesticForeign: Literal["all", "foreign"] = Field(
        default="all", description='Use "foreign" to search all foreign locations. Otherwise use "all"'
    )
    selectedFundingAgencies: dict[str, Any] = Field(default_factory=dict)
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: list[str] = Field(default_factory=list)
    recipientDomesticForeign: Literal["all", "foreign"] = Field(
        default="all", description='Use "foreign" to search all foreign locations. Otherwise use "all"'
    )
    recipientType: list[RecipientType] = Field(
        default_factory=list,
        description=(
            "Recipient type filter for award recipients. Select one or more types from the categories below.\n\n"
            "GENERAL BUSINESS (10 types) - For-profit entities:\n"
            "  business - Any business entity\n"
            "  small_business - Small business (SBA size standards)\n"
            "  other_than_small_business - Large businesses\n"
            "  corporate_entity_tax_exempt - Tax-exempt corporations\n"
            "  corporate_entity_not_tax_exempt - Taxable corporations\n"
            "  partnership_or_limited_liability_partnership - Partnerships/LLPs\n"
            "  sole_proprietorship - Individual-owned businesses\n"
            "  manufacturer_of_goods - Manufacturing companies\n"
            "  subchapter_s_corporation - S-Corps (pass-through taxation)\n"
            "  limited_liability_corporation - LLCs\n\n"
            "MINORITY OWNED BUSINESS (11 types) - Businesses owned by racial/ethnic minorities:\n"
            "  minority_owned_business - Any minority-owned business\n"
            "  alaskan_native_corporation_owned_firm - Alaska Native corporations\n"
            "  american_indian_owned_business - American Indian owned\n"
            "  asian_pacific_american_owned_business - Asian Pacific American owned\n"
            "  black_american_owned_business - Black/African American owned\n"
            "  hispanic_american_owned_business - Hispanic/Latino owned\n"
            "  native_american_owned_business - Native American owned\n"
            "  native_hawaiian_organization_owned_firm - Native Hawaiian organizations\n"
            "  subcontinent_asian_indian_american_owned_business - South Asian owned\n"
            "  tribally_owned_firm - Tribal government-owned\n"
            "  other_minority_owned_business - Other minority categories\n\n"
            "WOMEN OWNED BUSINESS (5 types) - Businesses owned/controlled by women:\n"
            "  woman_owned_business - Any women-owned business\n"
            "  women_owned_small_business - Women-owned small business (WOSB)\n"
            "  economically_disadvantaged_women_owned_small_business - Economically disadvantaged WOSB (EDWOSB)\n"
            "  joint_venture_women_owned_small_business - WOSB joint ventures\n"
            "  joint_venture_economically_disadvantaged_women_owned_small_business - EDWOSB joint ventures\n\n"
            "VETERAN OWNED BUSINESS (2 types) - Businesses owned by military veterans:\n"
            "  veteran_owned_business - Veteran-owned business (VOB)\n"
            "  service_disabled_veteran_owned_business - Service-disabled veteran-owned (SDVOB)\n\n"
            "SPECIAL DESIGNATIONS (20 types) - Businesses with federal program certifications:\n"
            "  special_designations - Any special designation\n"
            "  8a_program_participant - SBA 8(a) Business Development program\n"
            "  ability_one_program - AbilityOne (employs people with disabilities)\n"
            "  dot_certified_disadvantaged_business_enterprise - DoT DBE certified\n"
            "  emerging_small_business - Emerging small business\n"
            "  federally_funded_research_and_development_corp - FFRDCs\n"
            "  historically_underutilized_business_firm - HUBZone certified\n"
            "  labor_surplus_area_firm - Located in labor surplus areas\n"
            "  sba_certified_8a_joint_venture - SBA-certified 8(a) joint ventures\n"
            "  self_certified_small_disadvanted_business - Self-certified small disadvantaged business\n"
            "  small_agricultural_cooperative - Agricultural cooperatives\n"
            "  community_developed_corporation_owned_firm - Community development corporations\n"
            "  us_owned_business - U.S.-owned businesses\n"
            "  foreign_owned_and_us_located_business - Foreign-owned, U.S.-based\n"
            "  foreign_owned - Foreign-owned entities\n"
            "  foreign_government - Foreign government entities\n"
            "  international_organization - International organizations (UN, World Bank, etc.)\n"
            "  domestic_shelter - Domestic violence shelters\n"
            "  hospital - Hospital facilities\n"
            "  veterinary_hospital - Veterinary hospitals\n\n"
            "NONPROFIT (3 types) - Tax-exempt organizations:\n"
            "  nonprofit - Any nonprofit organization (501(c) entities)\n"
            "  foundation - Private/public foundations\n"
            "  community_development_corporations - Community development nonprofits\n\n"
            "HIGHER EDUCATION (6 types) - Colleges and universities:\n"
            "  higher_education - Any higher education institution\n"
            "  public_institution_of_higher_education - Public colleges/universities\n"
            "  private_institution_of_higher_education - Private colleges/universities\n"
            "  minority_serving_institution_of_higher_education - MSIs (HBCUs, HSIs, TCUs, etc.)\n"
            "  school_of_forestry - Forestry schools\n"
            "  veterinary_college - Veterinary medicine schools\n\n"
            "GOVERNMENT (10 types) - Government entities:\n"
            "  government - Any government entity\n"
            "  national_government - Federal government agencies\n"
            "  interstate_entity - Multi-state compacts/authorities\n"
            "  regional_and_state_government - State governments\n"
            "  regional_organization - Regional planning organizations\n"
            "  us_territory_or_possession - Puerto Rico, Guam, USVI, etc.\n"
            "  council_of_governments - Regional councils (COGs)\n"
            "  local_government - Cities, counties, municipalities\n"
            "  indian_native_american_tribal_government - Federally recognized tribes\n"
            "  authorities_and_commissions - Public authorities/commissions\n\n"
            "INDIVIDUALS (1 type) - Individual recipients:\n"
            "  individuals - Individual persons (grants, scholarships, etc.)\n\n"
            "Usage examples:\n"
            "  All small businesses: ['small_business']\n"
            "  Women and minority-owned: ['woman_owned_business', 'minority_owned_business']\n"
            "  Veterans and 8(a): ['veteran_owned_business', '8a_program_participant']\n"
            "  All nonprofits and education: ['nonprofit', 'higher_education']\n"
            "  State and local government: ['regional_and_state_government', 'local_government']\n"
            "  HUBZone small businesses: ['historically_underutilized_business_firm', 'small_business']"
        ),
    )
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
    defCodes: DEFCodeLists = Field(
        default_factory=DEFCodeLists,
        description=(
            "Disaster/Emergency Fund Codes (DEFC) filter using CodeLists structure with 'require' and 'exclude' lists.\n\n"
            "VALID CODES ONLY: A-Z, 1-9, AAA-AAJ, Q, QQQ\n\n"
            "Common groupings:\n"
            "COVID-19 Pandemic (use all 7 for comprehensive COVID spending):\n"
            "  L - Coronavirus Preparedness (P.L. 116-123, Mar 2020)\n"
            "  M - Families First Act (P.L. 116-127, Mar 2020)\n"
            "  N - CARES Act (P.L. 116-136, Mar 2020)\n"
            "  O - Non-emergency COVID (multiple P.L.s)\n"
            "  P - Paycheck Protection Program (P.L. 116-139, Apr 2020)\n"
            "  U - Consolidated Appropriations 2021 COVID (P.L. 116-260, Dec 2020)\n"
            "  V - American Rescue Plan (P.L. 117-2, Mar 2021)\n\n"
            "Infrastructure Investment and Jobs Act (IIJA):\n"
            "  Z - Emergency IIJA funding (P.L. 117-58, Nov 2021)\n"
            "  1 - Non-emergency IIJA funding (P.L. 117-58, Nov 2021)\n\n"
            "Ukraine Aid:\n"
            "  6 - Additional Ukraine Supplemental (P.L. 117-128, May 2022)\n"
            "  AAA - Ukraine Continuing Appropriations (P.L. 117-180, Sep 2022)\n\n"
            "2017-2020 Natural Disasters:\n"
            "  A - P.L. 115-56 (Sep 2017 - Hurricanes Harvey, Irma, Maria)\n"
            "  B - P.L. 115-72 (Oct 2017 - Additional disaster relief)\n"
            "  C - Bipartisan Budget Act 2018 (P.L. 115-123, Feb 2018)\n"
            "  D - FAA Reauthorization (P.L. 115-254, Oct 2018)\n"
            "  E - Additional Disaster Relief 2019 (P.L. 116-20, Jun 2019)\n"
            "  F - Southern Border Assistance (P.L. 116-26, Jul 2019)\n"
            "  G - Emergency Consolidated Appropriations 2020 (P.L. 116-93, Dec 2019)\n"
            "  H - Disaster Consolidated Appropriations 2020 (P.L. 116-93, Dec 2019)\n"
            "  I - Further Consolidated Appropriations 2020 (P.L. 116-94, Dec 2019)\n"
            "  J - Wildfire Suppression (P.L. 116-94, Dec 2019)\n"
            "  K - USMCA Implementation (P.L. 116-113, Jan 2020)\n\n"
            "2021-2024 Appropriations:\n"
            "  W - Emergency Security Supplemental (P.L. 117-31, Jul 2021)\n"
            "  X - Emergency Extending Government Funding (P.L. 117-43, Sep 2021)\n"
            "  Y - Disaster Extending Government Funding (P.L. 117-43, Sep 2021)\n"
            "  2 - Further Extending Government Funding (P.L. 117-70, Dec 2021)\n"
            "  3 - Emergency Consolidated Appropriations 2022 (P.L. 117-103, Mar 2022)\n"
            "  4 - Disaster Consolidated Appropriations 2022 (P.L. 117-103, Mar 2022)\n"
            "  5 - Wildfire Suppression 2022 (P.L. 117-103, Mar 2022)\n"
            "  7 - Bipartisan Safer Communities Act (P.L. 117-159, Jun 2022)\n"
            "  8 - Legislative Branch Appropriations (P.L. 117-167, Aug 2022)\n"
            "  AAB - Emergency Consolidated Appropriations 2023 (P.L. 117-328, Dec 2022)\n"
            "  AAC - Wildfire Suppression 2023 (P.L. 117-328, Dec 2022)\n"
            "  AAD - Disaster Consolidated Appropriations 2023 (P.L. 117-328, Dec 2022)\n"
            "  AAE - Continuing Appropriations 2024 (P.L. 118-15, Sep 2023)\n"
            "  AAF - Emergency Consolidated Appropriations 2024 (P.L. 118-42, Mar 2024)\n"
            "  AAG - Disaster Consolidated Appropriations 2024 (P.L. 118-42, Mar 2024)\n"
            "  AAH - Emergency P.L. 118-47 (2024)\n"
            "  AAI - Disaster P.L. 118-47 (2024)\n"
            "  AAJ - Emergency P.L. 118-50 (2024)\n\n"
            "Special codes:\n"
            "  Q - Not Designated (non-emergency/non-disaster)\n"
            "  9 - Unspecified non-COVID (discontinued Jul 2021)\n"
            "  QQQ - Excluded from tracking\n\n"
            "Usage examples:\n"
            "  All COVID spending: {'require': ['L', 'M', 'N', 'O', 'P', 'U', 'V']}\n"
            "  Infrastructure only: {'require': ['Z', '1']}\n"
            "  Exclude COVID from results: {'exclude': ['L', 'M', 'N', 'O', 'P', 'U', 'V']}\n"
            "  2017 hurricanes: {'require': ['A', 'B']}\n"
            "  Ukraine aid: {'require': ['6', 'AAA']}"
        ),
    )
    defCode: list[str] = Field(
        default_factory=list,
        description=(
            "Legacy DEFC filter (list of codes). Use defCodes (CodeLists) instead for require/exclude logic.\n"
            "Valid codes: A-Z, 1-9, AAA-AAJ, QQQ (see defCodes description for details)"
        ),
    )
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