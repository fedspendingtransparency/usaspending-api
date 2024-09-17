import json
from copy import deepcopy
from datetime import datetime, MINYEAR, MAXYEAR
from django.conf import settings
from typing import Optional

from usaspending_api.awards.models import Award
from usaspending_api.awards.v2.lookups.lookups import (
    all_subaward_types,
    assistance_type_mapping,
    award_type_mapping,
    contract_type_mapping,
    idv_type_mapping,
    grant_type_mapping,
    direct_payment_type_mapping,
    loan_type_mapping,
    other_type_mapping,
)
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.download.helpers import get_date_range_length
from usaspending_api.download.lookups import (
    FILE_FORMATS,
    VALID_ACCOUNT_SUBMISSION_TYPES,
)
from usaspending_api.references.models import DisasterEmergencyFundCode, ToptierAgency
from usaspending_api.submissions import helpers as sub_helpers
from usaspending_api.submissions.helpers import get_last_closed_submission_date


class DownloadValidatorBase:
    name: str

    def __init__(self, request_data: dict):
        self.common_tinyshield_models = [
            {
                "name": "columns",
                "key": "columns",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
                "min": 0,
            },
            {
                "name": "file_format",
                "key": "file_format",
                "type": "enum",
                "enum_values": FILE_FORMATS.keys(),
                "default": "csv",
            },
        ]
        self._json_request = {
            "file_format": request_data.get("file_format", "csv").lower(),
        }
        if request_data.get("columns"):
            self._json_request["columns"] = request_data.get("columns")

        self.tinyshield_models = []

    def get_validated_request(self):
        models = self.tinyshield_models + self.common_tinyshield_models
        validated_request = TinyShield(models).block(self._json_request)
        validated_request["request_type"] = self.name
        return validated_request

    def set_filter_defaults(self, defaults: dict):
        for key, val in defaults.items():
            self._json_request["filters"].setdefault(key, val)

    @property
    def json_request(self):
        return deepcopy(self._json_request)


class AwardDownloadValidator(DownloadValidatorBase):
    name = "award"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.request_data = request_data
        self._json_request["download_types"] = self.request_data.get("award_levels")
        self._json_request["filters"] = _validate_filters_exist(request_data)
        self.set_filter_defaults({"award_type_codes": list(award_type_mapping.keys())})

        constraint_type = self.request_data.get("constraint_type")
        if constraint_type == "year" and sorted(self._json_request["filters"]) == ["award_type_codes", "keywords"]:
            self._handle_keyword_search_download()
        elif constraint_type == "year":
            self._handle_custom_award_download()
        elif constraint_type == "row_count":
            self._handle_advanced_search_download()
        else:
            raise InvalidParameterException('Invalid parameter: constraint_type must be "row_count" or "year"')

    def _handle_keyword_search_download(self):
        # Overriding all other filters if the keyword filter is provided in year-constraint download
        self._json_request["filters"] = {"transaction_keyword_search": self._json_request["filters"]["keywords"]}

        self.tinyshield_models.extend(
            [
                {
                    "name": "transaction_keyword_search",
                    "key": "filters|transaction_keyword_search",
                    "type": "array",
                    "array_type": "text",
                    "text_type": "search",
                },
                {
                    "name": "download_types",
                    "key": "download_types",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": ["prime_awards"],
                },
            ]
        )
        self._json_request = self.get_validated_request()
        self._json_request["limit"] = settings.MAX_DOWNLOAD_LIMIT
        self._json_request["filters"]["award_type_codes"] = list(award_type_mapping)

    def _handle_custom_award_download(self):
        """
        Custom Award Download allows different filters than other Award Download Endpoints
        and thus it needs to be normalized before moving forward
        # TODO: Refactor to use similar filters as Advanced Search download
        """
        self.tinyshield_models.extend(
            [
                {
                    "name": "agencies",
                    "key": "filters|agencies",
                    "type": "array",
                    "array_type": "object",
                    "object_keys": {
                        "type": {"type": "enum", "enum_values": ["funding", "awarding"], "optional": False},
                        "tier": {"type": "enum", "enum_values": ["toptier", "subtier"], "optional": False},
                        "toptier_name": {"type": "text", "text_type": "search", "optional": True},
                        "name": {"type": "text", "text_type": "search", "optional": False},
                    },
                },
                {"name": "agency", "key": "filters|agency", "type": "integer"},
                {
                    "name": "date_range",
                    "key": "filters|date_range",
                    "type": "object",
                    "optional": False,
                    "object_keys": {
                        "start_date": {"type": "date", "default": "1000-01-01"},
                        "end_date": {"type": "date", "default": datetime.strftime(datetime.utcnow(), "%Y-%m-%d")},
                    },
                },
                {
                    "name": "date_type",
                    "key": "filters|date_type",
                    "type": "enum",
                    "enum_values": ["action_date", "last_modified_date"],
                    "default": "action_date",
                },
                {
                    "name": "place_of_performance_locations",
                    "key": "filters|place_of_performance_locations",
                    "type": "array",
                    "array_type": "object",
                    "object_keys": {
                        "country": {"type": "text", "text_type": "search", "optional": False},
                        "state": {"type": "text", "text_type": "search", "optional": True},
                        "zip": {"type": "text", "text_type": "search", "optional": True},
                        "district_original": {
                            "type": "text",
                            "text_type": "search",
                            "optional": True,
                            "text_min": 2,
                            "text_max": 2,
                        },
                        "district_current": {
                            "type": "text",
                            "text_type": "search",
                            "optional": True,
                            "text_min": 2,
                            "text_max": 2,
                        },
                        "county": {"type": "text", "text_type": "search", "optional": True},
                        "city": {"type": "text", "text_type": "search", "optional": True},
                    },
                },
                {
                    "name": "place_of_performance_scope",
                    "key": "filters|place_of_performance_scope",
                    "type": "enum",
                    "enum_values": ["domestic", "foreign"],
                },
                {
                    "name": "prime_award_types",
                    "key": "filters|prime_award_types",
                    "type": "array",
                    "array_type": "enum",
                    "min": 0,
                    "enum_values": list(award_type_mapping.keys()),
                },
                {
                    "name": "recipient_locations",
                    "key": "filters|recipient_locations",
                    "type": "array",
                    "array_type": "object",
                    "object_keys": {
                        "country": {"type": "text", "text_type": "search", "optional": False},
                        "state": {"type": "text", "text_type": "search", "optional": True},
                        "zip": {"type": "text", "text_type": "search", "optional": True},
                        "district_original": {
                            "type": "text",
                            "text_type": "search",
                            "optional": True,
                            "text_min": 2,
                            "text_max": 2,
                        },
                        "district_current": {
                            "type": "text",
                            "text_type": "search",
                            "optional": True,
                            "text_min": 2,
                            "text_max": 2,
                        },
                        "county": {"type": "text", "text_type": "search", "optional": True},
                        "city": {"type": "text", "text_type": "search", "optional": True},
                    },
                },
                {
                    "name": "recipient_scope",
                    "key": "filters|recipient_scope",
                    "type": "enum",
                    "enum_values": ("domestic", "foreign"),
                },
                {"name": "sub_agency", "key": "filters|sub_agency", "type": "text", "text_type": "search"},
                {
                    "name": "sub_award_types",
                    "key": "filters|sub_award_types",
                    "type": "array",
                    "array_type": "enum",
                    "min": 0,
                    "enum_values": all_subaward_types,
                },
            ]
        )

        filter_all_agencies = False
        if str(self._json_request["filters"].get("agency", "")).lower() == "all":
            filter_all_agencies = True
            self._json_request["filters"].pop("agency")

        self._json_request = self.get_validated_request()
        custom_award_filters = self._json_request["filters"]
        final_award_filters = {}

        # These filters do not need any normalization
        for key, value in custom_award_filters.items():
            if key in [
                "recipient_locations",
                "recipient_scope",
                "place_of_performance_locations",
                "place_of_performance_scope",
            ]:
                final_award_filters[key] = value

        if get_date_range_length(custom_award_filters["date_range"]) > 366:
            raise InvalidParameterException("Invalid Parameter: date_range total days must be within a year")

        final_award_filters["time_period"] = [
            {**custom_award_filters["date_range"], "date_type": custom_award_filters["date_type"]}
        ]

        if (
            custom_award_filters.get("prime_award_types") is None
            and custom_award_filters.get("sub_award_types") is None
        ):
            raise InvalidParameterException(
                "Missing one or more required body parameters: prime_award_types or sub_award_types"
            )

        self._json_request["download_types"] = []
        final_award_filters["prime_and_sub_award_types"] = {}

        if custom_award_filters.get("prime_award_types"):
            self._json_request["download_types"].append("prime_awards")
            final_award_filters["prime_and_sub_award_types"]["prime_awards"] = custom_award_filters["prime_award_types"]

        if custom_award_filters.get("sub_award_types"):
            self._json_request["download_types"].append("elasticsearch_sub_awards")
            final_award_filters["prime_and_sub_award_types"]["elasticsearch_sub_awards"] = custom_award_filters[
                "sub_award_types"
            ]

        if "agency" in custom_award_filters:
            if "agencies" not in custom_award_filters:
                final_award_filters["agencies"] = []

            if filter_all_agencies:
                toptier_name = "all"
            else:
                toptier_name = (
                    ToptierAgency.objects.filter(toptier_agency_id=custom_award_filters["agency"])
                    .values("name")
                    .first()
                )
                if toptier_name is None:
                    raise InvalidParameterException(f"Toptier ID not found: {custom_award_filters['agency']}")
                toptier_name = toptier_name["name"]

            if "sub_agency" in custom_award_filters:
                final_award_filters["agencies"].append(
                    {
                        "type": "awarding",
                        "tier": "subtier",
                        "name": custom_award_filters["sub_agency"],
                        "toptier_name": toptier_name,
                    }
                )
            else:
                final_award_filters["agencies"].append({"type": "awarding", "tier": "toptier", "name": toptier_name})

        if "agencies" in custom_award_filters:
            final_award_filters["agencies"] = [
                val for val in custom_award_filters["agencies"] if val.get("name", "").lower() != "all"
            ]

        self._json_request["filters"] = final_award_filters

    def _handle_advanced_search_download(self):
        self.tinyshield_models.extend(
            [
                *AWARD_FILTER_NO_RECIPIENT_ID,
                {
                    "name": "limit",
                    "key": "limit",
                    "type": "integer",
                    "min": 0,
                    "max": settings.MAX_DOWNLOAD_LIMIT,
                    "default": settings.MAX_DOWNLOAD_LIMIT,
                },
                {
                    "name": "download_types",
                    "key": "download_types",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": [
                        "elasticsearch_awards",
                        "elasticsearch_sub_awards",
                        "elasticsearch_transactions",
                        "prime_awards",
                    ],
                },
            ]
        )
        self._json_request["limit"] = self.request_data.get("limit", settings.MAX_DOWNLOAD_LIMIT)
        self._json_request = self.get_validated_request()


class IdvDownloadValidator(DownloadValidatorBase):
    name = "idv"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.tinyshield_models.extend(
            [
                {
                    "key": "award_id",
                    "name": "award_id",
                    "type": "any",
                    "models": [{"type": "integer"}, {"type": "text", "text_type": "raw"}],
                    "optional": False,
                    "allow_nulls": False,
                },
                {
                    "name": "limit",
                    "key": "limit",
                    "type": "integer",
                    "min": 0,
                    "max": settings.MAX_DOWNLOAD_LIMIT,
                    "default": settings.MAX_DOWNLOAD_LIMIT,
                },
            ]
        )
        self._json_request = request_data
        self._json_request = self.get_validated_request()
        award_id, piid, _, _, _ = _validate_award_id(self._json_request.pop("award_id"))
        filters = {
            "idv_award_id": award_id,
            "award_type_codes": tuple(set(contract_type_mapping) | set(idv_type_mapping)),
        }
        self._json_request.update(
            {
                "account_level": "treasury_account",
                "download_types": ["idv_orders", "idv_transaction_history", "idv_federal_account_funding"],
                "include_file_description": {
                    "source": settings.IDV_DOWNLOAD_README_FILE_PATH,
                    "destination": "readme.txt",
                },
                "piid": piid,
                "is_for_idv": True,
                "filters": filters,
                "include_data_dictionary": True,
            }
        )


class ContractDownloadValidator(DownloadValidatorBase):
    name = "contract"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.tinyshield_models.extend(
            [
                {
                    "key": "award_id",
                    "name": "award_id",
                    "type": "any",
                    "models": [{"type": "integer"}, {"type": "text", "text_type": "raw"}],
                    "optional": False,
                    "allow_nulls": False,
                },
                {
                    "name": "limit",
                    "key": "limit",
                    "type": "integer",
                    "min": 0,
                    "max": settings.MAX_DOWNLOAD_LIMIT,
                    "default": settings.MAX_DOWNLOAD_LIMIT,
                },
            ]
        )
        self._json_request = request_data
        self._json_request = self.get_validated_request()
        award_id, piid, _, _, _ = _validate_award_id(self._json_request.pop("award_id"))
        filters = {
            "award_id": award_id,
            "award_type_codes": tuple(set(contract_type_mapping)),
        }
        self._json_request.update(
            {
                "account_level": "treasury_account",
                "download_types": ["sub_contracts", "contract_transactions", "contract_federal_account_funding"],
                "include_file_description": {
                    "source": settings.CONTRACT_DOWNLOAD_README_FILE_PATH,
                    "destination": "ContractAwardSummary_download_readme.txt",
                },
                "award_id": award_id,
                "piid": piid,
                "is_for_idv": False,
                "is_for_contract": True,
                "is_for_assistance": False,
                "filters": filters,
                "include_data_dictionary": True,
            }
        )


class AssistanceDownloadValidator(DownloadValidatorBase):
    name = "assistance"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.tinyshield_models.extend(
            [
                {
                    "key": "award_id",
                    "name": "award_id",
                    "type": "any",
                    "models": [{"type": "integer"}, {"type": "text", "text_type": "raw"}],
                    "optional": False,
                    "allow_nulls": False,
                },
                {
                    "name": "limit",
                    "key": "limit",
                    "type": "integer",
                    "min": 0,
                    "max": settings.MAX_DOWNLOAD_LIMIT,
                    "default": settings.MAX_DOWNLOAD_LIMIT,
                },
            ]
        )
        self._json_request = request_data
        self._json_request = self.get_validated_request()
        award_id, _, fain, uri, generated_unique_award_id = _validate_award_id(self._json_request.pop("award_id"))
        filters = {
            "award_id": award_id,
            "award_type_codes": tuple(set(assistance_type_mapping)),
        }
        award = fain
        if "AGG" in generated_unique_award_id:
            award = uri

        self._json_request.update(
            {
                "account_level": "treasury_account",
                "download_types": ["assistance_transactions", "sub_grants", "assistance_federal_account_funding"],
                "include_file_description": {
                    "source": settings.ASSISTANCE_DOWNLOAD_README_FILE_PATH,
                    "destination": "AssistanceAwardSummary_download_readme.txt",
                },
                "award_id": award_id,
                "assistance_id": award,
                "is_for_idv": False,
                "is_for_contract": False,
                "is_for_assistance": True,
                "filters": filters,
                "include_data_dictionary": True,
            }
        )


class DisasterRecipientDownloadValidator(DownloadValidatorBase):
    name = "disaster_recipient"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.tinyshield_models.extend(
            [
                {
                    "key": "filters|def_codes",
                    "name": "def_codes",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": sorted(DisasterEmergencyFundCode.objects.values_list("code", flat=True)),
                    "allow_nulls": False,
                    "optional": False,
                },
                {
                    "key": "filters|query",
                    "name": "query",
                    "type": "text",
                    "text_type": "search",
                    "allow_nulls": False,
                    "optional": True,
                },
                {
                    "key": "filters|award_type_codes",
                    "name": "award_type_codes",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": sorted(award_type_mapping.keys()),
                    "allow_nulls": False,
                    "optional": True,
                },
            ]
        )
        self._json_request["filters"] = request_data.get("filters")
        self._json_request = self.get_validated_request()
        self._json_request["download_types"] = [self.name]

        # Determine what to use in the filename based on "award_type_codes" filter;
        # Also add "face_value_of_loans" column if only loan types
        award_category = "All-Awards"
        award_type_codes = set(self._json_request["filters"].get("award_type_codes", award_type_mapping.keys()))
        columns = ["recipient", "award_obligations", "award_outlays", "number_of_awards"]

        if award_type_codes <= set(contract_type_mapping.keys()):
            award_category = "Contracts"
        elif award_type_codes <= set(idv_type_mapping.keys()):
            award_category = "Contract-IDVs"
        elif award_type_codes <= set(grant_type_mapping.keys()):
            award_category = "Grants"
        elif award_type_codes <= set(loan_type_mapping.keys()):
            award_category = "Loans"
            columns.insert(3, "face_value_of_loans")
        elif award_type_codes <= set(direct_payment_type_mapping.keys()):
            award_category = "Direct-Payments"
        elif award_type_codes <= set(other_type_mapping.keys()):
            award_category = "Other-Financial-Assistance"

        self._json_request["award_category"] = award_category
        self._json_request["columns"] = self._json_request.get("columns") or tuple(columns)

        # Need to specify the field to use "query" filter on if present
        query_text = self._json_request["filters"].pop("query", None)
        if query_text:
            self._json_request["filters"]["query"] = {"text": query_text, "fields": ["recipient_name"]}


class AccountDownloadValidator(DownloadValidatorBase):
    name = "account"

    def __init__(self, request_data: dict):
        super().__init__(request_data)
        self.tinyshield_models.extend(
            [
                {
                    "name": "account_level",
                    "key": "account_level",
                    "type": "enum",
                    "enum_values": ["federal_account", "treasury_account"],
                    "optional": False,
                },
                {
                    "name": "fy",
                    "key": "filters|fy",
                    "type": "integer",
                    "min": MINYEAR,
                    "max": MAXYEAR,
                    "optional": False,
                },
                {"name": "quarter", "key": "filters|quarter", "type": "integer", "min": 1, "max": 4},
                {"name": "period", "key": "filters|period", "type": "integer", "min": 2, "max": 12},
                {
                    "name": "submission_type",
                    "key": "filters|submission_type",
                    "type": "enum",
                    "enum_values": VALID_ACCOUNT_SUBMISSION_TYPES,
                },
                {
                    "name": "submission_types",
                    "key": "filters|submission_types",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": VALID_ACCOUNT_SUBMISSION_TYPES,
                },
                {"name": "agency", "key": "filters|agency", "type": "text", "text_type": "search", "default": "all"},
                {
                    "name": "def_codes",
                    "key": "filters|def_codes",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": sorted(DisasterEmergencyFundCode.objects.values_list("code", flat=True)),
                },
                {
                    "name": "federal_account",
                    "key": "filters|federal_account",
                    "type": "text",
                    "text_type": "search",
                    "default": "all",
                },
                {
                    "name": "budget_function",
                    "key": "filters|budget_function",
                    "type": "text",
                    "text_type": "search",
                    "default": "all",
                },
                {
                    "name": "budget_subfunction",
                    "key": "filters|budget_subfunction",
                    "type": "text",
                    "text_type": "search",
                    "default": "all",
                },
            ]
        )
        self._json_request["account_level"] = request_data.get("account_level")
        self._json_request["filters"] = request_data.get("filters", {})
        self._json_request = self.get_validated_request()

        fy = self._json_request["filters"].get("fy")
        quarter = self._json_request["filters"].get("quarter")
        period = self._json_request["filters"].get("period")

        fy, quarter, period = _validate_and_bolster_requested_submission_window(fy, quarter, period)

        _validate_submission_type(self._json_request["filters"])

        self._json_request["filters"]["fy"] = fy
        self._json_request["filters"]["quarter"] = quarter
        self._json_request["filters"]["period"] = period
        self._json_request["download_types"] = self._json_request["filters"]["submission_types"]


class DisasterDownloadValidator(DownloadValidatorBase):
    name = "disaster"

    def __init__(self, request_date: dict):
        super().__init__(request_date)

        covid_defc = list(
            DisasterEmergencyFundCode.objects.filter(group_name="covid_19")
            .order_by("code")
            .values_list("code", flat=True)
        )
        self.tinyshield_models.extend(
            [
                {
                    "name": "def_codes",
                    "key": "filters|def_codes",
                    "type": "array",
                    "array_type": "enum",
                    "enum_values": covid_defc,
                    "allow_nulls": False,
                    "optional": True,
                    "default": covid_defc,
                },
            ]
        )
        self._json_request["filters"] = request_date.get("filters")
        self._json_request = self.get_validated_request()

        # Add all Award Type Codes to filters to support current Award and Subaward download logic
        self._json_request["filters"]["award_type_codes"] = list(award_type_mapping)

        # Limit the DEFC options to either a complete group or a single DEFC;
        defc_filter = sorted(self._json_request["filters"]["def_codes"])
        if len(defc_filter) > 1:
            if set(defc_filter) == set(covid_defc):
                self._json_request["pre_generated_download"] = {
                    "name_match": settings.COVID19_DOWNLOAD_FILENAME_PREFIX,
                    "request_match": f'"def_codes": {json.dumps(covid_defc)}',
                }
            else:
                raise InvalidParameterException(
                    "The Disaster Download is currently limited to either all COVID-19 DEFC or a single COVID-19 DEFC."
                )

        # Add Date filters to allow correct filename for account CSVs
        latest = get_last_closed_submission_date(is_quarter=False)

        self._json_request.update(
            {
                "account_level": "treasury_account",
                "account_filters": {
                    "is_multi_year": True,
                    "latest_fiscal_year": latest["submission_fiscal_year"],
                    "latest_fiscal_period": latest["submission_fiscal_month"],
                    "start_date": "2020-04-01",
                },
                "download_types": [
                    "gtas_balances",
                    "object_class_program_activity",
                    "elasticsearch_awards",
                    "elasticsearch_sub_awards",
                ],
                "include_data_dictionary": True,
                "include_file_description": {
                    "source": settings.COVID19_DOWNLOAD_README_FILE_PATH,
                    "destination": "COVID-19_download_readme.txt",
                },
            }
        )


def _validate_award_id(award_id):
    if type(award_id) is int or award_id.isdigit():
        filters = {"id": int(award_id)}
    else:
        filters = {"generated_unique_award_id": award_id}

    award = (
        Award.objects.filter(**filters).values_list("id", "piid", "fain", "uri", "generated_unique_award_id").first()
    )
    if not award:
        raise InvalidParameterException("Unable to find award matching the provided award id")
    return award


def _validate_filters_exist(request_data):
    filters = request_data.get("filters")
    if not isinstance(filters, dict):
        raise InvalidParameterException("Filters parameter not provided as a dict")
    elif len(filters) == 0:
        raise InvalidParameterException("At least one filter is required.")
    return filters


def _validate_and_bolster_requested_submission_window(
    fy: int, quarter: Optional[int], period: Optional[int]
) -> (int, Optional[int], Optional[int]):
    """
    The assumption here is that each of the provided values has been validated independently already.
    Now it's time to validate them as a pair.  We also need to bolster period or quarter since they
    are mutually exclusive in the filter.
    """
    if quarter is None and period is None:
        raise InvalidParameterException("Either 'period' or 'quarter' is required in filters.")

    if quarter is not None and period is not None:
        raise InvalidParameterException("Supply either 'period' or 'quarter' in filters but not both.")

    if period is not None:
        # If period is provided, then we are going to grab the most recently closed quarter in the
        # same fiscal year equal to or less than the period requested.  If there are no closed
        # quarters in the fiscal year matching this criteria then no quarterly submissions will be
        # returned.  So, by way of example, if the user requests P7 and Q2 is closed then we will
        # return P7 monthly and Q2 quarterly submissions.  If Q2 is not closed yet, we will return
        # P7 monthly and Q1 quarterly submissions.  If P2 is requested then we will only return P2
        # monthly submissions since there can be no closed quarter prior to P2 in the same year.
        # Finally, if P3 is requested and Q1 is closed then we will return P3 monthly and Q1 quarterly
        # submissions.  Man I hope that all made sense.
        quarter = sub_helpers.get_last_closed_quarter_relative_to_month(fy, period)

    else:
        # This is the same idea as above, the big difference being that we do not have monthly
        # submissions for earlier years so really this will either return the final period of
        # the quarter or None.
        period = sub_helpers.get_last_closed_month_relative_to_quarter(fy, quarter)

    return fy, quarter, period


def _validate_submission_type(filters: dict) -> None:
    """Validate submission_type/submission_types parameter

    In February 2020 submission_type became the legacy filter, replaced by submission_types
    submission_type was left in-place for backward compatibility but hidden in API Contract and error messages
    """
    legacy_submission_type = filters.get("submission_type", ...)
    submission_types = filters.get("submission_types", ...)

    if submission_types == ... and legacy_submission_type == ...:
        raise InvalidParameterException("Missing required filter: submission_types")

    elif submission_types == ... and legacy_submission_type != ...:
        del filters["submission_type"]
        if isinstance(legacy_submission_type, list):
            raise InvalidParameterException("Use filter `submission_types` to request multiple submission types")
        else:
            submission_types = [legacy_submission_type]
    else:
        if not isinstance(submission_types, list):
            submission_types = [submission_types]

    if len(submission_types) == 0:
        msg = f"Provide at least one value in submission_types: {' '.join(VALID_ACCOUNT_SUBMISSION_TYPES)}"
        raise InvalidParameterException(msg)

    if any(True for submission_type in submission_types if submission_type not in VALID_ACCOUNT_SUBMISSION_TYPES):
        msg = f"Invalid value in submission_types. Options: [{', '.join(VALID_ACCOUNT_SUBMISSION_TYPES)}]"
        raise InvalidParameterException(msg)

    filters["submission_types"] = list(set(submission_types))
