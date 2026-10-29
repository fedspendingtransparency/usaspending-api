import re
from abc import ABC, abstractmethod
from datetime import date
from functools import cached_property
from typing import Annotated, Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, computed_field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from usaspending_api.awards.v2.lookups.lookups import all_awards_types_to_category
from usaspending_api.common.helpers.orm_helpers import award_types_are_valid_groups
from usaspending_api.llm.tools.helpers.code_counts import build_naics_counts, build_psc_counts, build_tas_counts
from usaspending_api.llm.tools.helpers.code_paths import build_psc_path, build_tas_path
from usaspending_api.references.helpers import aget_defc_code_details, get_defc_code_details
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS


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


class AIToolResult(BaseModel):
    instructions: str = Field(default="", description="Instructions for llm in event of certain tool return cases")
    results: dict = Field(default_factor=dict, description="Tool output to be incorporated into resulting filter")


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
        "ZIP Code",
    ] = Field(description="The type of geographic entity")
    standalone: str = Field(description="Short location name for filter chips (e.g., 'Texas', 'Chicago', '64198')")
    title: str = Field(
        description="Display name for the location filter UI. Cities include state (e.g., 'KANSAS CITY, MISSOURI')"
    )


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

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, value: str) -> str:
        """Dates must be parseable in YYYY-MM-DD format."""
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"'{value}' is not a valid date; dates must be in YYYY-MM-DD format.") from exc
        return value


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

    @field_validator("abbreviation", mode="before")
    @classmethod
    def default_missing_abbreviation(cls, value: str | None) -> str:
        """Some subtier agencies have no abbreviation in the DB (lookup_agencies returns None
        for them). The frontend reads this field unconditionally, so it must always be a
        string -- fall back to "" rather than rejecting the input."""
        return value or ""


class SelectedAgency(BaseModel):
    """Model for a selected agency"""

    id: int
    toptier_flag: bool
    toptier_agency: ToptierAgency
    subtier_agency: SubtierAgency | None = None
    agencyType: Literal["toptier", "subtier"] = "toptier"

    @model_validator(mode="after")
    def default_subtier_agency(self) -> "SelectedAgency":
        """Ensure subtier_agency is always populated

        If a subtier agency is not provided use the toptier agency's own name/abbreviation into
        subtier_agency rather than leaving it None -- which, combined with `exclude_none=True`
        on serialization, drops the key entirely and causes as error. A subtier selection, on
        the other hand, must supply its own subtier_agency; it can't be defaulted.
        """
        if self.subtier_agency is None:
            if self.agencyType == "subtier":
                raise ValueError("subtier_agency is required when agencyType='subtier'.")
            self.subtier_agency = SubtierAgency(
                abbreviation=self.toptier_agency.abbreviation, name=self.toptier_agency.name
            )
        return self


class SelectedCFDA(BaseModel):
    """Model for a selected CFDA (Assistance Listing) program, keyed by its program number."""

    identifier: str
    program_number: str
    program_title: str
    popular_name: str | None = None

    @model_validator(mode="after")
    def identifier_matches_program_number(self) -> "SelectedCFDA":
        if self.identifier != self.program_number:
            raise PydanticCustomError(
                "identifier_mismatch", "'identifier' must equal 'program_number' for a selected CFDA."
            )
        return self


def _validate_cfda_keys_match_identifier(value: dict[str, SelectedCFDA]) -> dict[str, SelectedCFDA]:
    """Shared check for Filters/ExecuteFilterInput: selectedCFDA must be keyed by its own identifier."""
    mismatched = [key for key, cfda in value.items() if key != cfda.identifier]
    if mismatched:
        raise PydanticCustomError(
            "cfda_key_mismatch",
            "Each selectedCFDA key must equal its entry's 'identifier' (the CFDA program number).",
            {"mismatched_keys": mismatched},
        )
    return value


class CodeDisplay(BaseModel):
    """Summary of one root node (e.g. a NAICS sector, PSC Tier1 group, or TAS agency) selected by a CodeLists.

    These are only ever calculated from the codes in 'require' and 'exclude' (see tools/helpers/code_counts.py);
    they are never provided by the llm.
    """

    model_config = ConfigDict(frozen=True)

    count: Annotated[
        int,
        Field(
            gt=0,
            description=(
                "The number of leaf nodes under the root node that remain after applying 'require' and 'exclude'"
            ),
            json_schema_extra={"examples": [279]},
        ),
    ]
    label: Annotated[
        str,
        Field(
            description="The name or description of the root node", json_schema_extra={"examples": ["Manufacturing"]}
        ),
    ]
    value: Annotated[
        str,
        Field(description="The code of the root node", json_schema_extra={"examples": ["33"]}),
    ]


class CodeLists(BaseModel, ABC):
    """Base model for code lists, e.g. NAICS, PSC, and TAS codes.

    The shape of 'require'/'exclude' differs by code type (flat codes for NAICS, hierarchical paths for PSC/TAS), so
    each subclass declares its own typed fields along with the validation for them. The base class owns 'counts'.

    'counts' is a computed field rather than an input: it is derived from 'require' and 'exclude' so it can't
    disagree with them. Computed fields are left out of the validation JSON schema, which means the llm is never asked
    to provide it, and any value included in the input is ignored. It is included when the model is dumped.

    Calculating 'counts' queries the database with the synchronous ORM. When dumping from async code, run the dump
    through sync_to_async.
    """

    require: list
    exclude: list

    @abstractmethod
    def _build_counts(self) -> list[dict[str, Any]]:
        """Return the count details ('value', 'label', and 'count') for each root node left after 'exclude'."""

    @computed_field(
        description="Details about the root nodes represented by the codes in 'require'.",
        json_schema_extra={"examples": [[{"count": 279, "label": "Manufacturing", "value": "33"}]]},
    )
    @cached_property
    def counts(self) -> list[CodeDisplay]:
        return [CodeDisplay(**count) for count in self._build_counts()]


class NaicsCodeLists(CodeLists):
    require: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="List of NAICS codes that must be present.",
            json_schema_extra={"examples": [["336411", "336412"]]},
        ),
    ]
    exclude: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="List of NAICS codes to exclude.",
            json_schema_extra={"examples": [["336413"]]},
        ),
    ]

    def _build_counts(self) -> list[dict[str, Any]]:
        return build_naics_counts(self.require, self.exclude)


class PscCodeLists(CodeLists):
    require: Annotated[
        list[list[str]],
        Field(
            default_factory=list,
            description=(
                "List of PSC paths. Each path is [Tier1 group label, ...PSC code segments]. The first "
                "element must be one of the PSC Tier1 group labels ('Research and Development', "
                "'Service', 'Product'); any remaining elements are alphanumeric PSC codes."
            ),
            json_schema_extra={"examples": [[["Research and Development", "AC"], ["Service", "D"]]]},
        ),
    ]
    exclude: Annotated[
        list[list[str]],
        Field(
            default_factory=list,
            description="List of PSC paths to exclude, same shape as 'require'.",
            json_schema_extra={"examples": [[["Service", "D", "D3"]]]},
        ),
    ]

    @field_validator("require", "exclude", mode="before")
    @classmethod
    def rebuild_psc_paths(cls, paths: Any) -> Any:
        """A path is fully determined by its last code, so rebuild it rather than trusting the llm to assemble every
        tier correctly (e.g. ["Research and Development", "AA11"] becomes [..., "AA", "AA1", "AA11"]). Anything that
        doesn't look like a path ending in a code is left alone for the validation below to reject."""
        if not isinstance(paths, list):
            return paths

        code_pattern = re.compile(r"[A-Z0-9]{1,4}")
        return [
            build_psc_path(path[-1])
            if isinstance(path, list)
            and len(path) > 1
            and all(isinstance(segment, str) for segment in path)
            and code_pattern.fullmatch(path[-1])
            else path
            for path in paths
        ]

    @field_validator("require", "exclude", mode="after")
    @classmethod
    def validate_psc_paths(cls, paths: list[list[str]]) -> list[list[str]]:
        code_pattern = re.compile(r"[A-Z0-9]{1,4}")
        for path in paths:
            if not path or path[0] not in PSC_GROUPS:
                raise PydanticCustomError(
                    "invalid_psc_root",
                    f"The first element of each PSC path must be one of: {[str(key) for key in PSC_GROUPS]}.",
                    {"path": path},
                )
            invalid_codes = [code for code in path[1:] if not code_pattern.fullmatch(code)]
            if invalid_codes:
                raise PydanticCustomError(
                    "invalid_psc_code",
                    "PSC codes must be one to four character uppercased alphanumeric strings.",
                    {"codes": invalid_codes},
                )
        return paths

    def _build_counts(self) -> list[dict[str, Any]]:
        return build_psc_counts(self.require, self.exclude)


class TasCodeLists(CodeLists):
    require: Annotated[
        list[list[str]],
        Field(
            default_factory=list,
            description=(
                "List of hierarchical TAS paths, each 1-3 elements: [agency_code] or "
                "[agency_code, agency_code-main_account, full_tas_rendering_label]."
            ),
            json_schema_extra={"examples": [[["310"], ["535", "535-2724", "535-2020/2021-2724-000"]]]},
        ),
    ]
    exclude: Annotated[
        list[list[str]],
        Field(
            default_factory=list,
            description="List of hierarchical TAS paths to exclude, same shape as 'require'.",
            json_schema_extra={"examples": [[["535", "535-2724"]]]},
        ),
    ]

    @field_validator("require", "exclude", mode="before")
    @classmethod
    def rebuild_tas_paths(cls, paths: Any) -> Any:
        """Rebuild full paths from their TAS rendering label, which determines the agency and AID-MAIN above it,
        rather than trusting the llm to assemble them (e.g. ["310", "310-2012", "310-2012/2012-3200-000"] becomes
        ["310", "310-3200", "310-2012/2012-3200-000"]). Shorter paths are left as they are."""
        if not isinstance(paths, list):
            return paths

        rebuilt = []
        for path in paths:
            if isinstance(path, list) and len(path) == 3 and all(isinstance(segment, str) for segment in path):
                full_path = build_tas_path(path[2])
                path = full_path if len(full_path) == 3 else path
            rebuilt.append(path)
        return rebuilt

    @field_validator("require", "exclude", mode="after")
    @classmethod
    def validate_tas_paths(cls, paths: list[list[str]]) -> list[list[str]]:
        invalid_paths = [path for path in paths if not path or len(path) > 3]
        if invalid_paths:
            raise PydanticCustomError(
                "invalid_tas_path", "Each TAS path must have between 1 and 3 elements.", {"paths": invalid_paths}
            )
        return paths

    def _build_counts(self) -> list[dict[str, Any]]:
        return build_tas_counts(self.require, self.exclude)


DEFC_GROUPS = ("covid_19", "infrastructure")

# Shared by get_defc_rows()/aget_defc_rows() -- hits the DB once per process (whichever of the sync/async
# getters is called first), then every subsequent call (sync or async) just reads this cache. A plain
# module-level cache is used instead of @lru_cache since lru_cache can't safely work with async
_defc_rows_cache: tuple[dict, ...] | None = None


def clear_defc_rows_cache() -> None:
    """Test helper: reset the process-wide DEFC rows cache shared by get_defc_rows()/aget_defc_rows()."""
    global _defc_rows_cache
    _defc_rows_cache = None


def get_defc_rows() -> tuple[dict, ...]:
    """DEFC rows for the frontend-supported groups; hits the DB once per process, then cached.

    Sync entry point -- used by the (necessarily synchronous) Pydantic validator in DEFCodeLists.
    See aget_defc_rows() for the async-safe counterpart.
    """
    global _defc_rows_cache
    if _defc_rows_cache is None:
        _defc_rows_cache = tuple(get_defc_code_details(list(DEFC_GROUPS)))
    return _defc_rows_cache


async def aget_defc_rows() -> tuple[dict, ...]:
    """Async counterpart to get_defc_rows(); safe to call from an async context."""
    global _defc_rows_cache
    if _defc_rows_cache is None:
        _defc_rows_cache = tuple(await aget_defc_code_details(list(DEFC_GROUPS)))
    return _defc_rows_cache


def get_valid_defc_codes() -> frozenset[str]:
    return frozenset(row["code"] for row in get_defc_rows())


def _format_defc_description(rows: tuple[dict, ...]) -> str:
    labels = {"covid_19": "COVID-19", "infrastructure": "Infrastructure"}
    lines = [
        "Disaster/Emergency Fund Codes (DEFC) filter with 'require'/'exclude' lists. Only COVID-19 and "
        "Infrastructure DEFCs are supported (matching what the Advanced Search page allows). Use codes "
        "from the matching group:",
    ]
    rows_by_group: dict[str, list[dict]] = {}
    for row in rows:
        rows_by_group.setdefault(row["group_name"], []).append(row)
    for group_name in DEFC_GROUPS:
        group_rows = rows_by_group.get(group_name, [])
        codes = ", ".join(f"'{row['code']}'" for row in group_rows)
        lines.append(f"  {labels.get(group_name, group_name)}: [{codes}]")
        for row in group_rows:
            lines.append(f"    {row['code']} = {row['title']} ({row['public_law']})")
    return "\n".join(lines)


def build_defc_description() -> str:
    """LLM-facing description for the defCodes field, built from the DB rows above."""
    return _format_defc_description(get_defc_rows())


async def abuild_defc_description() -> str:
    """Async counterpart to build_defc_description(); safe to call from an async context."""
    return _format_defc_description(await aget_defc_rows())


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

SetAsideCode = Literal[
    "NONE",
    "SBA",
    "SBP",
    "RSB",
    "VSB",
    "ESB",
    "8A",
    "8AN",
    "8AC",
    "HZC",
    "HZS",
    "HS2",
    "HS3",
    "SDVOSBC",
    "SDVOSBS",
    "VSA",
    "VSS",
    "WOSB",
    "WOSBSS",
    "EDWOSB",
    "EDWOSBSS",
    "HMT",
    "HMP",
    "BI",
    "IEE",
    "ISBEE",
]

ExtentCompetedCode = Literal[
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "CDO",
    "NDO",
]


class DEFCodeLists(BaseModel):
    """Validation model for DEFC code lists"""

    require: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)

    @field_validator("require", "exclude")
    @classmethod
    def validate_def_codes(cls, value: list[str]) -> list[str]:
        """Validate DEFC codes against the DB-sourced, supported set (COVID-19 + Infrastructure)."""
        if not value:
            return value
        valid_codes = get_valid_defc_codes()
        unknown = [code for code in value if code not in valid_codes]
        if unknown:
            raise ValueError(f"Invalid DEFC code(s): {unknown}. See the defCodes field description for valid codes.")
        return value


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
    selectedFundingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(default_factory=dict)
    selectedRecipients: list[str] = Field(default_factory=list, max_length=50)
    recipientDomesticForeign: Literal["all", "foreign"] = "all"
    recipientType: list[RecipientType] = Field(default_factory=list)
    selectedRecipientLocations: dict[str, Any] = Field(default_factory=dict)
    awardType: list[str] = Field(default_factory=list)
    selectedAwardIDs: list[str] = Field(default_factory=list)
    awardAmounts: AwardAmounts = Field(default_factory=lambda: AwardAmounts({}))
    selectedCFDA: dict[str, SelectedCFDA] = Field(default_factory=dict)
    naicsCodes: NaicsCodeLists = Field(default_factory=NaicsCodeLists)
    pscCodes: PscCodeLists = Field(default_factory=PscCodeLists)
    defCode: list[str] = []
    defCodes: DEFCodeLists = Field(default_factory=DEFCodeLists)
    pricingType: list[str] = Field(default_factory=list)
    setAside: list[SetAsideCode] = Field(default_factory=list)
    extentCompeted: list[ExtentCompetedCode] = Field(default_factory=list)
    tasCodes: TasCodeLists = Field(default_factory=TasCodeLists)
    awardDescription: str = ""
    filterNewAwardsOnlySelected: bool = False
    filterNewAwardsOnlyActive: bool = False
    filterNaoActiveFromFyOrDateRange: bool = False

    @field_validator("selectedAwardingAgencies", "selectedFundingAgencies", mode="before")
    @classmethod
    def rekey_selected_agencies(cls, value: Any) -> Any:
        """Re-key agency dicts by "{id}_{agencyType}".

        The frontend keys selected agencies by "{id}_{agencyType}" (e.g. "1173_toptier"), but the
        LLM does not reliably reproduce that key. Since each value already carries `id` and
        `agencyType`, rebuild the key from the value so the persisted filter is always correctly
        keyed regardless of what key the model emitted.
        """
        if not isinstance(value, dict):
            return value

        rekeyed = {}
        for original_key, agency in value.items():
            if isinstance(agency, dict):
                agency_id = agency.get("id")
                agency_type = agency.get("agencyType")
            else:
                agency_id = getattr(agency, "id", None)
                agency_type = getattr(agency, "agencyType", None)
            # Fall back to the original key if the value is missing the pieces we need; the
            # SelectedAgency validation below will then surface the real problem.
            key = f"{agency_id}_{agency_type}" if agency_id is not None and agency_type else original_key
            rekeyed[key] = agency
        return rekeyed

    @field_validator("selectedCFDA", mode="after")
    @classmethod
    def validate_selected_cfda(cls, value: dict[str, SelectedCFDA]) -> dict[str, SelectedCFDA]:
        return _validate_cfda_keys_match_identifier(value)

    @field_validator("timePeriodFY")
    @classmethod
    def validate_time_period_fy(cls, value: list[str]) -> list[str]:
        """Fiscal years must be bare four-digit strings (e.g. '2026'), not 'FY2026'."""
        invalid = [year for year in value if not re.fullmatch(r"\d{4}", year)]
        if invalid:
            raise ValueError(
                f"Invalid fiscal year(s) in timePeriodFY: {invalid}. Fiscal years must be four-digit strings "
                "(e.g. '2026'), without a 'FY' prefix."
            )
        return value

    @field_validator("awardType")
    @classmethod
    def validate_award_type(cls, value: list[str]) -> list[str]:
        """Validate award-type codes: each must be a known code, and all must share one award group."""
        if not value:
            return value
        unknown = [code for code in value if code not in all_awards_types_to_category]
        if unknown:
            raise ValueError(
                f"Invalid award type code(s): {unknown}. See the awardType field description for valid codes."
            )
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
    """Version of DEFCodeLists without its DB-backed validator.
    This reduces the payload send to the llm with every call."""

    require: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="DEFC codes that must be present. See the defCodes field description for valid codes.",
            json_schema_extra={"examples": [["L", "M", "N"]]},
        ),
    ]
    exclude: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="DEFC codes to exclude. See the defCodes field description for valid codes.",
            json_schema_extra={"examples": [["Z"]]},
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
        description=(
            "Free-text keywords. Only for discrete terms with no matching structured filter. For literal "
            "descriptive phrases, product/item names, addresses, or multi-term OR-style searches, use "
            "awardDescription instead (comma-separate terms there for OR logic). Never put boolean or "
            "HTML-escaped syntax (e.g. '&quot;jeep&quot; OR &quot;toyota&quot;') in either field."
        ),
        json_schema_extra={"examples": [["bridge", "repair"]]},
    )
    timePeriodType: Literal["fy", "dr"] = Field(
        default="fy",
        description=(
            "Time period mode: 'fy' populates timePeriodFY; 'dr' populates time_period. Populate only one. "
            "Leave BOTH timePeriodFY and time_period empty/omitted unless the query names or clearly implies "
            "a year, date, or date range — do not default to the current year or to all available years just "
            "because no time period was mentioned. Use 'fy' only when the query names one or more whole "
            "federal fiscal years (e.g. 'in FY2023', 'since 2022'). Use 'dr' for anything involving quarters, "
            "months, or explicit calendar dates. The federal fiscal year runs Oct 1 - Sep 30 of the following "
            "calendar year, so a fiscal quarter must be converted to calendar dates for 'dr': FY Q1 = "
            "Oct 1 - Dec 31 (previous calendar year), Q2 = Jan 1 - Mar 31, Q3 = Apr 1 - Jun 30, Q4 = Jul 1 - "
            "Sep 30 (all calendar-year dates matching the fiscal year's number). Example: 'Q2 FY2024' -> "
            'timePeriodType=\'dr\', time_period=[{"start_date": "2024-01-01", "end_date": "2024-03-31"}] '
            "(NOT a calendar-year Q2). For relative phrases ('last year', 'this quarter'), compute the actual "
            "dates from the current date given in the system prompt."
        ),
    )
    timePeriodFY: Annotated[
        list[str],
        Field(
            description=(
                "Fiscal years as four-digit strings. Only when timePeriodType='fy'. Leave empty unless the query names "
                "specific fiscal year(s)."
            ),
            json_schema_extra={"examples": [["2023", "2024"]], "pattern": "^\\d{4}$"},
        ),
    ] = []
    time_period: Annotated[
        list[TimePeriod],
        Field(
            default_factory=list,
            description=(
                "Custom date ranges (YYYY-MM-DD). Only when timePeriodType='dr'. Leave empty unless the query "
                "implies specific dates, months, or quarters. See timePeriodType's description for how to "
                "convert fiscal quarters to calendar dates."
            ),
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
    selectedAwardingAgencies: dict[str, SelectedAgency] = Field(
        default_factory=dict,
        description=(
            'Awarding agencies keyed by "{id}_{agencyType}" (e.g. "1173_toptier"). Must call the '
            "lookup_agency tool to attain valid selected agency objects; pass the tool's returned "
            "dictionary through unchanged, preserving its keys. Prefer awarding agency filter over funding agency "
            "filter."
        ),
    )
    selectedFundingAgencies: dict[str, SelectedAgency] = Field(
        default_factory=dict,
        description=(
            'Funding agencies keyed by "{id}_{agencyType}" (e.g. "1173_toptier"). Must call the '
            "lookup_agency tool to attain valid selected agency objects; pass the tool's returned "
            "dictionary through unchanged, preserving its keys."
        ),
    )

    selectedRecipients: list[str] = Field(
        default_factory=list,
        max_length=50,
        description=(
            "Named recipients only (specific companies/organizations), resolved via the lookup_recipient "
            "tool. May include multiple recipients (up to 50). Do NOT use this for generic/demographic/category "
            "terms (e.g. 'veteran-owned', 'small business', 'minority-owned') - those belong in recipientType via "
            "list_recipient_types instead. Once a term has been resolved to a recipientType code, do not "
            "also call lookup_recipient for that same term."
        ),
        json_schema_extra={
            "examples": [
                ["LOCKHEED MARTIN CORPORATION"],
                ["HR1JA12FSM63"],
                ["BOEING", "SPACE EXPLORATION TECHNOLOGIES CORP."],
            ]
        },
    )
    recipientDomesticForeign: Literal["all", "foreign"] = Field(
        default="all", description='Use "foreign" to search all foreign recipient locations. Otherwise use "all".'
    )
    recipientType: list[str] = Field(
        default_factory=list,
        description=(
            "Business/organization type filter for award recipients (e.g. 'small_business'). "
            "Call list_recipient_types for all valid values grouped by category. Use this for "
            "generic/demographic/category terms (e.g. 'veteran-owned', 'minority-owned', 'small business') - "
            "do not treat these as named recipients for selectedRecipients/lookup_recipient."
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
            "Award-type code filter. If the query mentions an award vehicle (contract, grant, loan, IDV, direct "
            "payment, financial assistance, or a related word/concept), you MUST set this filter - even when the "
            "query's main subject is a recipient, agency, or location (e.g. 'contracts for Boeing' requires BOTH "
            "the recipient filter AND this filter). Use exactly one of these code groups, matching the query:\n"
            "  contracts:       ['A', 'B', 'C', 'D']\n"
            "  IDVs:            ['IDV_A', 'IDV_B', 'IDV_B_A', 'IDV_B_B', 'IDV_B_C', 'IDV_C', 'IDV_D', 'IDV_E']\n"
            "  grants:          ['02', '03', '04', '05', 'F001', 'F002']\n"
            "  loans:           ['07', '08', 'F003', 'F004']\n"
            "  direct payments: ['06', '10', 'F006', 'F007']\n"
            "  other:           ['09', '11', '-1', 'F005', 'F008', 'F009', 'F010']\n"
            "A value may only contain codes from a SINGLE group."
        ),
        json_schema_extra={"examples": [["A", "B", "C", "D"], ["02", "03", "04", "05"], ["07", "08"]]},
    )
    selectedAwardIDs: list[str] = Field(
        default_factory=list,
        description="Award ID (PIID/FAIN/URI) filter.",
        json_schema_extra={"examples": [["N0002417C2117"], ["N0002417C2117", "B-18-DP-72-0002"]]},
    )
    awardAmounts: dict[str, list[int | None]] = Field(
        default_factory=dict,
        description=(
            "Award amount ranges as {key: [min, max]}; None = unbounded (never use a large sentinel number "
            "like 999999999999 for an open-ended bound - use None). Predefined buckets have fixed "
            "bounds and are combinable: range-0 [,1M], range-1 [1M,25M], range-2 [25M,100M], "
            "range-3 [100M,500M], range-4 [500M,]. Only use range-N buckets when the query's thresholds "
            "match those exact boundaries. For ANY other arbitrary user-stated threshold (e.g. 'over $1 "
            "million', 'between $500k and $2M', 'at least $750,000'), use 'specific': [min, max] instead - "
            "'specific' must be the only key when used, and its bounds are NOT limited to the range-N "
            "boundaries."
        ),
        json_schema_extra={
            "examples": [
                {"range-0": [None, 1000000], "range-2": [25000000, 100000000]},
                {"specific": [5000000, 50000000]},
                {"specific": [1000000, None]},
            ]
        },
    )
    selectedCFDA: dict[str, SelectedCFDA] = Field(
        default_factory=dict,
        description=(
            "CFDA / Assistance Listing filter keyed by program number. Grants and assistance programs for a "
            "subject are found with the lookup_codes tool using code_type 'cfda'. Pass its selectedCFDA value "
            "through unchanged."
        ),
    )
    naicsCodes: NaicsCodeLists = Field(
        default_factory=NaicsCodeLists,
        description=(
            "Industry (NAICS) filter. Get the values from the lookup_codes tool with code_type 'naics' and pass "
            "them through unchanged."
        ),
    )
    pscCodes: PscCodeLists = Field(
        default_factory=PscCodeLists,
        description=(
            "Product and Service Code (PSC) filter for what is being purchased. Get the values from the "
            "lookup_codes tool with code_type 'psc' and pass them through unchanged."
        ),
    )
    defCodes: DEFCodeListsWithoutEnum = Field(
        default_factory=DEFCodeListsWithoutEnum,
        description=(
            "Disaster/Emergency Fund Codes (DEFC) filter with 'require'/'exclude' lists. Only COVID-19 and "
            "Infrastructure DEFCs are supported."
        ),
        json_schema_extra={
            "examples": [
                {"require": ["L", "M", "N", "O", "P", "U", "V"]},
                {"require": ["Z", "1"]},
            ]
        },
    )
    pricingType: list[str] = Field(default_factory=list, description="Contract pricing type codes (e.g. 'A', 'B').")
    setAside: list[str] = Field(
        default_factory=list,
        description=(
            "Type-of-set-aside codes. Valid codes:\n"
            "  NONE     No Set Aside Used\n"
            "  SBA      Small Business Set-Aside - Total\n"
            "  SBP      Small Business Set-Aside - Partial\n"
            "  RSB      Reserved for Small Business\n"
            "  VSB      Very Small Business Set-Aside\n"
            "  ESB      Emerging Small Business Set-Aside\n"
            "  8A       8A Competed\n"
            "  8AN      8(a) Sole Source\n"
            "  8AC      SDB Set-Aside 8(a)\n"
            "  HZC      HUBZone Set-Aside\n"
            "  HZS      HUBZone Sole Source\n"
            "  HS2      Combination HUBZone and 8(a)\n"
            "  HS3      8(a) with HUBZone Preference\n"
            "  SDVOSBC  Service-Disabled Veteran-Owned Small Business Set-Aside\n"
            "  SDVOSBS  SDVOSB Sole Source\n"
            "  VSA      Veteran Set-Aside\n"
            "  VSS      Veteran Sole Source\n"
            "  WOSB     Women-Owned Small Business\n"
            "  WOSBSS   Women Owned Small Business Sole Source\n"
            "  EDWOSB   Economically-Disadvantaged Women-Owned Small Business\n"
            "  EDWOSBSS Economically Disadvantaged Women Owned Small Business Sole Source\n"
            "  HMT      HBCU or MI Set-Aside - Total\n"
            "  HMP      HBCU or MI Set-Aside - Partial\n"
            "  BI       Buy Indian\n"
            "  IEE      Indian Economic Enterprise\n"
            "  ISBEE    Indian Small Business Economic Enterprise\n"
            "Use this field (not recipientType or selectedRecipients) for set-aside/socioeconomic "
            "program language like 'Native American owned', 'HUBZone', '8(a)', or 'SDVOSB set-aside'."
        ),
    )
    extentCompeted: list[str] = Field(
        default_factory=list,
        description=(
            "Extent-competed codes. Valid codes:\n"
            "  A    Full and Open Competition\n"
            "  B    Not Available for Competition\n"
            "  C    Not Competed\n"
            "  D    Full and Open Competition after exclusion of sources\n"
            "  E    Follow On to Competed Action\n"
            "  F    Competed under SAP\n"
            "  G    Not Competed under SAP\n"
            "  CDO  Competitive Delivery Order\n"
            "  NDO  Non-Competitive Delivery Order"
        ),
    )
    tasCodes: TasCodeLists = Field(
        default_factory=TasCodeLists,
        description=(
            "Treasury account (TAS) filter. Get the values from the lookup_codes tool with code_type 'tas' and "
            "pass them through unchanged."
        ),
    )
    awardDescription: str = Field(
        default="",
        description=(
            "Free-text award description search term for literal descriptive phrases, product/item names, "
            "or addresses meant to match verbatim. For multiple terms with OR logic, comma-separate them "
            "(e.g. 'jeep,toyota') - do not use boolean or HTML-escaped syntax."
        ),
    )
    filterNewAwardsOnlySelected: bool = Field(default=False, description="When true, limit results to new awards only.")
    filterNewAwardsOnlyActive: bool = Field(
        default=False, description="When true, the new-awards-only filter is active."
    )
    filterNaoActiveFromFyOrDateRange: bool = Field(
        default=False, description="When true, derive the new-awards-only window from the selected FY or date range."
    )

    @field_validator("selectedCFDA", mode="after")
    @classmethod
    def validate_selected_cfda(cls, value: dict[str, SelectedCFDA]) -> dict[str, SelectedCFDA]:
        return _validate_cfda_keys_match_identifier(value)


class FilterRequest(BaseModel):
    """Main model for the filter request"""

    filters: Filters
    version: str = "2020-06-01"


class FilterResponse(BaseModel):
    """Model for the API response"""

    hash: str


class FilterSearchInput(BaseModel):
    query: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="The natural language query describing what the user wants to search for",
        json_schema_extra={"examples": ["How many contracts did the Department of Energy award in fiscal year 2025?"]},
    )


class FilterSearchEvent(BaseModel):
    search_id: str | None = None
    tool_use_id: str | None = None
    type: Literal["search_start", "search_error", "search_complete", "tool_start", "tool_complete", "tool_error"]
    message: str
    result: Any = None
