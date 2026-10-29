import re
from datetime import date
from typing import Annotated, Any, Callable, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, field_validator, model_validator
from pydantic_core import PydanticCustomError

from usaspending_api.awards.v2.lookups.lookups import (
    all_awards_types_to_category,
    new_to_old_award_type_mapping,
    old_to_new_award_type_mapping,
)
from usaspending_api.llm.tools.helpers.hierarchy_parsers import get_naics_parent, get_tas_parent
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

    abbreviation: str = ""
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
    """Model containing details for how a collection of codes are displayed"""

    _get_parent: ClassVar[Callable[[str], str | None]]

    count: Annotated[
        int,
        Field(
            description="The number of leaf nodes under the root node represented by codes in 'require'",
            json_schema_extra={"examples": 279},
        ),
    ]
    label: Annotated[
        str,
        Field(
            description="The name or description on the root node represented by codes in 'require'",
            json_schema_extra={"examples": "Manufacturing"},
        ),
    ]
    value: Annotated[
        str,
        Field(
            description="The code on the root node represented by codes in 'require'",
            json_schema_extra={"examples": "33"},
        ),
    ]

    @model_validator(mode="after")
    def validate_code_and_label_are_different(self) -> "CodeDisplay":
        if self.value == self.label:
            raise PydanticCustomError(
                "incorrect_label",
                "The label cannot be the same as the value. It must be the description of the code",
            )
        return self

    @field_validator("value", mode="after")
    @classmethod
    def validate_is_root_code(cls, value: str) -> str:
        if cls._get_parent(value):
            raise PydanticCustomError(
                "root_code_required",
                "The code in 'value' must be a root node in its hierarchical tree",
            )
        return value

    @field_validator("count", mode="after")
    @classmethod
    def validate_non_zero_count(cls, value: int) -> int:
        if value == 0:
            raise PydanticCustomError("non_zero_count_required", "'count' must be greater than 0")
        return value


class NaicsCodeDisplay(CodeDisplay):
    _get_parent = staticmethod(get_naics_parent)


class PscCodeDisplay(CodeDisplay):
    """PSC's root is a synthetic Tier1 group label (see PSC_GROUPS), not a code with its own
    ancestor chain, so rootness is group membership rather than a parent-code lookup."""

    _get_parent = staticmethod(lambda code: None)

    @field_validator("value", mode="after")
    @classmethod
    def validate_is_psc_group_label(cls, value: str) -> str:
        if value not in PSC_GROUPS:
            raise PydanticCustomError(
                "invalid_psc_root",
                f"'value' must be one of the PSC Tier1 group labels: {[str(key) for key in PSC_GROUPS]}",
            )
        return value

    @field_validator("label", mode="after")
    @classmethod
    def validate_label_is_empty(cls, value: str) -> str:
        if value != "":
            raise PydanticCustomError("label_must_be_empty", "'label' must be an empty string for PSC Tier1 groups.")
        return value


class TasCodeDisplay(CodeDisplay):
    _get_parent = staticmethod(get_tas_parent)


def _counts_field(code_display_type: type[CodeDisplay]) -> Annotated:
    """Build the 'counts' field annotation, parameterized by the CodeDisplay subtype each CodeLists uses."""
    return Annotated[
        list[code_display_type],
        Field(
            description="List of details about the root nodes represented by the codes in 'require'.",
            json_schema_extra={"examples": [{"count": 279, "label": "Manufacturing", "value": "33"}]},
        ),
    ]


class CodeLists(BaseModel):
    """Base Model for code lists
    e.g. naics codes, psc codes, and tas codes

    'require'/'exclude' shape differs by code type (flat codes for NAICS, hierarchical paths for
    PSC/TAS), so each subclass declares its own typed 'require'/'exclude' and the matching
    'codes_in_require_exist_in_counts' validator. Only the shape-agnostic checks live here.
    """

    require: Annotated[list, Field(default_factory=list, description="List of codes/paths that must be present.")]
    exclude: Annotated[list, Field(default_factory=list, description="List of codes/paths to exclude.")]
    counts: _counts_field(CodeDisplay)

    @model_validator(mode="before")
    @classmethod
    def require_counts_when_other_values_present(cls, data: Any) -> Any:
        """Handle setting a default inside the validator as this is a conditionally required field"""
        if isinstance(data, dict):
            data["counts"] = data.get("counts", [])

            if not data.get("counts") and data.get("require"):
                msg = (
                    "There must be a unique entry in 'counts' that contains the code, name, and count of the leaf "
                    "nodes for each root node from the trees represented by the codes in 'require'. If two or more "
                    "codes in 'require' share a root node then the leaf count from each tree should be combined as "
                    "one total count. Don't change or remove a code from 'require'."
                )
                raise (PydanticCustomError("counts_required", msg))

        return data

    @field_validator("counts", mode="after")
    @classmethod
    def counts_contains_only_unique_nonzero_root_nodes(cls, code_display_list: list[CodeDisplay]) -> list[CodeDisplay]:
        unique_code_values = {}
        for code_display in code_display_list:
            if code_display.value in unique_code_values:
                unique_code_values[code_display.value] += 1
            else:
                unique_code_values[code_display.value] = 0

        if len(code_display_list) != len(unique_code_values):
            raise PydanticCustomError(
                "duplicate_codes_in_counts",
                "Their counts should be combined under a new entry that represents the root node.",
                {"duplicate_codes": [code for code in unique_code_values if unique_code_values[code] > 1]},
            )

        return code_display_list


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
    counts: _counts_field(NaicsCodeDisplay)

    @model_validator(mode="after")
    def codes_in_require_exist_in_counts(self) -> "NaicsCodeLists":
        counts_pattern = "|".join([re.escape(code_display.value) for code_display in self.counts])
        counts_pattern = f"^(?:{counts_pattern}).*$"

        missing_codes = [code for code in self.require if re.fullmatch(counts_pattern, code) is None]
        if missing_codes:
            raise PydanticCustomError(
                "missing_codes_in_counts",
                "'counts' must contain a code from each root node represented by the codes in 'require'.",
                {"missing_codes": missing_codes},
            )
        return self


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
    counts: _counts_field(PscCodeDisplay)

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

    @model_validator(mode="after")
    def codes_in_require_exist_in_counts(self) -> "PscCodeLists":
        valid_roots = {code_display.value for code_display in self.counts}
        missing_paths = [path for path in self.require if path[0] not in valid_roots]
        if missing_paths:
            raise PydanticCustomError(
                "missing_codes_in_counts",
                "'counts' must contain an entry for each PSC Tier1 group represented by the paths in 'require'.",
                {"missing_paths": missing_paths},
            )
        return self


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
    counts: _counts_field(TasCodeDisplay)

    @field_validator("require", "exclude", mode="after")
    @classmethod
    def validate_tas_paths(cls, paths: list[list[str]]) -> list[list[str]]:
        invalid_paths = [path for path in paths if not path or len(path) > 3]
        if invalid_paths:
            raise PydanticCustomError(
                "invalid_tas_path", "Each TAS path must have between 1 and 3 elements.", {"paths": invalid_paths}
            )
        return paths

    @model_validator(mode="after")
    def codes_in_require_exist_in_counts(self) -> "TasCodeLists":
        valid_roots = {code_display.value for code_display in self.counts}
        missing_paths = [path for path in self.require if path[0] not in valid_roots]
        if missing_paths:
            raise PydanticCustomError(
                "missing_codes_in_counts",
                "'counts' must contain an entry for each root node represented by the paths in 'require'.",
                {"missing_paths": missing_paths},
            )
        return self


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


def _with_legacy_codes(codes: Any) -> list[str]:
    """Expand award-type codes so historic awards (which kept their legacy numeric codes) still match:
    every code is normalized to its current code, then all legacy codes for that current code are added."""
    expanded = set()
    for code in codes:
        current = old_to_new_award_type_mapping.get(code, code)
        expanded.add(current)
        expanded.update(new_to_old_award_type_mapping.get(current, []))
    return sorted(expanded)


# Award-type names as shown in the Advanced Search frontend's award type filter.
AWARD_TYPE_GROUP_LABELS: dict[str, list[str]] = {
    "Contracts": ["A", "B", "C", "D"],
    "Contract IDVs": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"],
    "Grants": ["02", "03", "04", "F001"],
    "Cooperative Agreement": ["05", "F002"],
    "Direct Payments": ["06", "10", "F006", "F007"],
    "Loans": ["07", "08", "F003", "F004"],
    "Other": ["-1", "09", "11", "F005", "F008", "F009", "F010"],
}
ALL_AWARD_TYPE_CODES = frozenset(code for codes in AWARD_TYPE_GROUP_LABELS.values() for code in codes)
AWARD_TYPE_LABELS: dict[str, list[str]] = {
    # Not frontend checkboxes: shortcuts for selections the LLM otherwise spells out group by group.
    # "All Award Types" selects every code, which resolve_award_types collapses to no awardType filter.
    "All Award Types": sorted(ALL_AWARD_TYPE_CODES),
    "Financial Assistance": sorted(
        code
        for group in ("Grants", "Cooperative Agreement", "Direct Payments", "Loans")
        for code in AWARD_TYPE_GROUP_LABELS[group]
    ),
    **AWARD_TYPE_GROUP_LABELS,
    # Contracts
    "Blanket Purchase Agreements (BPA) Calls": ["A"],
    "Purchase Orders (PO)": ["B"],
    "Delivery Orders (DO)": ["C"],
    "Definitive Contracts": ["D"],
    # Contract IDVs
    "Government-Wide Acquisition Contract (GWAC)": ["IDV_A"],
    "Indefinite Delivery / Requirements Contract": ["IDV_B_A"],
    "Indefinite Delivery / Indefinite Quantity (IDIQ) Contract": ["IDV_B_B"],
    "Indefinite Delivery / Definite Quantity Contract": ["IDV_B_C"],
    "Federal Supply Schedule (FSS)": ["IDV_C"],
    "Basic Ordering Agreement (BOA)": ["IDV_D"],
    "Blanket Purchase Agreements (BPA)": ["IDV_E"],
    # Direct Payments
    "Direct Payment with Unrestricted Use": _with_legacy_codes(["F007"]),
    "Direct Payment for Specified Use": _with_legacy_codes(["F006"]),
    # Loans
    "Direct Loans": _with_legacy_codes(["F003"]),
    "Loan Guarantees": _with_legacy_codes(["F004"]),
    # Other
    "Indemnity/Insurance (non-loan)": _with_legacy_codes(["F005"]),
    "Asset Forfeiture/Equitable Sharing": _with_legacy_codes(["F008"]),
    "Sale, Exchange, or Donation of Property and Goods": _with_legacy_codes(["F009"]),
    "Other Financial Assistance": _with_legacy_codes(["F010"]),
    "Not Specified": ["-1"],
}
AwardTypeLabel = Literal[tuple(AWARD_TYPE_LABELS)]
_AWARD_TYPE_LABELS_BY_KEY = {" ".join(label.lower().split()): codes for label, codes in AWARD_TYPE_LABELS.items()}


def resolve_award_types(values: list[Any]) -> list[str]:
    """Resolve award-type names (or raw codes) to the sorted, legacy-expanded codes they select.

    Selecting every award group is the same as not filtering by award type, so it resolves to [].
    """
    codes: set[str] = set()
    unknown = []
    for value in values:
        key = " ".join(value.lower().split()) if isinstance(value, str) else None
        if key in _AWARD_TYPE_LABELS_BY_KEY:
            codes.update(_AWARD_TYPE_LABELS_BY_KEY[key])
        elif value in all_awards_types_to_category:
            codes.update(_with_legacy_codes([value]))
        else:
            unknown.append(value)
    if unknown:
        raise ValueError(
            f"Unknown awardType value(s): {unknown}. Use the award type names listed in the awardType "
            "description (e.g. 'Contracts', 'Grants', 'Direct Loans'), or omit awardType to include all award types."
        )
    if codes >= ALL_AWARD_TYPE_CODES:
        return []
    return sorted(codes)


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
    treasuryAccounts: dict[str, Any] = Field(default_factory=dict)
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

    @field_validator("awardType", mode="before")
    @classmethod
    def resolve_award_type(cls, value: Any) -> Any:
        """The LLM selects award types by frontend name (see AWARD_TYPE_LABELS); persist them as codes. Raw codes
        are still accepted so code-based callers and previously persisted filters keep working."""
        if not isinstance(value, list) or not value:
            return value
        return resolve_award_types(value)

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
    awardType: list[AwardTypeLabel] = Field(
        default_factory=list,
        description=(
            "Award type filter, using the award type names below. Set it when the query names a specific award "
            "vehicle (contracts, IDVs, grants, cooperative agreements, loans, direct payments, insurance, etc.) \n"
            "Shortcuts:\n"
            "  'All Award Types': every award type (the same as leaving awardType empty).\n"
            "  'Financial Assistance': every non-contract type (Grants, Cooperative Agreement, Direct Payments, "
            "Loans, Other). Use it for 'financial assistance' or 'assistance awards'.\n"
            "Use a group name to select every type in that group, or individual names for specific types:\n"
            "  'Contracts': 'Blanket Purchase Agreements (BPA) Calls', 'Purchase Orders (PO)', "
            "'Delivery Orders (DO)', 'Definitive Contracts'\n"
            "  'Contract IDVs': 'Government-Wide Acquisition Contract (GWAC)', "
            "'Indefinite Delivery / Requirements Contract', "
            "'Indefinite Delivery / Indefinite Quantity (IDIQ) Contract', "
            "'Indefinite Delivery / Definite Quantity Contract', 'Federal Supply Schedule (FSS)', "
            "'Basic Ordering Agreement (BOA)', 'Blanket Purchase Agreements (BPA)'\n"
            "  'Grants' (no sub-types)\n"
            "  'Cooperative Agreement' (no sub-types)\n"
            "  'Direct Payments': 'Direct Payment with Unrestricted Use', 'Direct Payment for Specified Use'\n"
            "  'Loans': 'Direct Loans', 'Loan Guarantees'\n"
            "  'Other': 'Indemnity/Insurance (non-loan)', 'Asset Forfeiture/Equitable Sharing', "
            "'Sale, Exchange, or Donation of Property and Goods', 'Other Financial Assistance', 'Not Specified'\n"
            "'Grants' does not include cooperative agreements; only add 'Cooperative Agreement' when the query asks "
            "for cooperative agreements. 'Blanket Purchase Agreements (BPA) Calls' is a contract type; "
            "'Blanket Purchase Agreements (BPA)' is an IDV type. Combine names from different groups when the query "
            "names several vehicles (e.g. 'grants and loans' -> ['Grants', 'Loans'])."
        ),
        json_schema_extra={
            "examples": [["Contracts"], ["Grants", "Loans"], ["Direct Loans"], ["Financial Assistance"]]
        },
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
        description="CFDA / Assistance Listing filter keyed by program number. Use the lookup_code tool.",
    )
    naicsCodes: NaicsCodeLists = Field(default_factory=NaicsCodeLists)
    pscCodes: PscCodeLists = Field(default_factory=PscCodeLists)
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
    treasuryAccounts: dict[str, Any] = Field(
        default_factory=dict, description="Treasury Account Symbol (TAS) filter keyed by identifier."
    )
    tasCodes: TasCodeLists = Field(default_factory=TasCodeLists)
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
