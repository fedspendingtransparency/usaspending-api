import logging
import json
from typing import List

from django.conf import settings
from django.utils.functional import cached_property
from rest_framework.exceptions import NotFound
from rest_framework.views import APIView

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.dict_helpers import update_list_of_dictionaries
from usaspending_api.common.helpers.fiscal_year_helpers import (
    calculate_last_completed_fiscal_quarter,
    get_final_period_of_quarter,
    current_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import get_account_data_time_period_message
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns
from usaspending_api.references.models import ToptierAgency, Agency
from usaspending_api.submissions.helpers import validate_request_within_revealed_submissions

logger = logging.getLogger(__name__)


class AgencyBase(APIView):

    params_to_validate: List[str]
    additional_models: dict

    def _validate_params(self, param_values, params_to_validate=None):
        params_to_validate = params_to_validate or getattr(self, "params_to_validate", [])
        additional_models = getattr(self, "additional_models", [])
        award_type_codes = sorted(award_type_mapping.keys())
        all_models = [
            {
                "key": "fiscal_year",
                "name": "fiscal_year",
                "type": "integer",
                "min": fy(settings.API_SEARCH_MIN_DATE),
                "max": current_fiscal_year(),
            },
            {
                "key": "fiscal_period",
                "name": "fiscal_period",
                "type": "integer",
                "min": 2,
                "max": 12,
            },
            {"key": "filter", "name": "filter", "type": "text", "text_type": "search"},
            {
                "key": "agency_type",
                "name": "agency_type",
                "type": "enum",
                "enum_values": ["awarding", "funding"],
                "optional": True,
                "default": "awarding",
            },
            {
                "name": "award_type_codes",
                "key": "award_type_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": award_type_codes + ["no intersection"],
                "optional": True,
            },
        ]
        all_models = update_list_of_dictionaries(all_models, additional_models, "key")

        # Empty strings cause issues with TinyShield
        for val in params_to_validate:
            if param_values.get(val) == "":
                param_values.pop(val)

        chosen_models = [model for model in all_models if model["key"] in params_to_validate]
        param_values = TinyShield(chosen_models).block(param_values)

        if param_values.get("fiscal_year"):
            validate_request_within_revealed_submissions(
                fiscal_year=param_values["fiscal_year"], fiscal_period=param_values.get("fiscal_period")
            )
        return param_values

    @cached_property
    def _query_params(self):
        query_params = self.request.query_params.copy()
        # ensure that `award_type_codes` is an array and not a string
        if query_params.get("award_type_codes") is not None:
            query_params["award_type_codes"] = query_params["award_type_codes"].strip("[]").split(",")
        return self._validate_params(query_params)

    @cached_property
    def validated_url_params(self):
        """
        Used by endpoints that need to validate URL parameters instead of or in addition to the query parameters.
        "additional_models" is used when a TinyShield models outside of the common ones above are needed.
        """
        return self._validate_params(self.kwargs, list(self.kwargs))

    @property
    def toptier_code(self):
        # We don't have to do any validation here because Django has already checked this to be
        # either a three or four digit numeric string based on the regex pattern in our route url.
        return self.kwargs["toptier_code"]

    @cached_property
    def agency_id(self):
        agency = Agency.objects.filter(toptier_flag=True, toptier_agency=self.toptier_agency).values("id")
        if not agency:
            raise NotFound(f"Cannot find Agency for toptier code of '{self.toptier_code}'")
        return agency[0]["id"]

    @cached_property
    def toptier_agency(self):
        toptier_agency = ToptierAgency.objects.filter(
            toptieragencypublisheddabsview__toptier_code=self.toptier_code
        ).first()
        if not toptier_agency:
            raise NotFound(f"Agency with a toptier code of '{self.toptier_code}' does not exist")
        return toptier_agency

    @cached_property
    def fiscal_year(self):
        return self._query_params.get("fiscal_year") or current_fiscal_year()

    @cached_property
    def fiscal_period(self):
        """
        This is the fiscal period we want to limit our queries to when querying CPE values for
        self.fiscal_year.  If it's prior to Q1 submission window close date, we will return
        quarter 1 anyhow and just show what we have (which will likely be incomplete).
        """
        return (
            self._query_params.get("fiscal_period")
            or get_final_period_of_quarter(calculate_last_completed_fiscal_quarter(self.fiscal_year))
            or 3
        )

    @property
    def filter(self):
        return self._query_params.get("filter")

    @property
    def agency_type(self):
        return self._query_params.get("agency_type")

    @property
    def award_type_codes(self):
        return self._query_params.get("award_type_codes")

    @property
    def standard_response_messages(self):
        return [get_account_data_time_period_message()] if self.fiscal_year < 2017 else []

    @staticmethod
    def create_assurance_statement_url(result):
        """
        Results requires the following keys to generate the assurance statement url:
        agency_name, abbreviation, toptier_code, fiscal_year, fiscal_period, submission_is_quarter
        """
        try:
            agency_name_split = result["agency_name"].split(" ")
            abbreviation_wrapped = f"({result['abbreviation']})"
            toptier_code = result["toptier_code"]
            fiscal_year = result["fiscal_year"]

            if result["submission_is_quarter"]:
                fiscal_period = f"Q{int(result['fiscal_period'] / 3)}"
            else:
                fiscal_period = f"P{str(result['fiscal_period']).zfill(2)}"
        except Exception:
            logger.error("Missing fields in result. Can't create assurance statement url.")
            logger.error(f"Result object includes: {json.dumps(result)}")
            return None

        host = settings.FILES_SERVER_BASE_URL
        file_name = (
            f"{fiscal_year}-{fiscal_period}-{toptier_code}_"
            + "%20".join([*agency_name_split, abbreviation_wrapped])
            + "-Agency_Comments.txt"
        )
        return f"{host}/agency_submissions/{file_name}"


class PaginationMixin:
    @cached_property
    def pagination(self):
        model = customize_pagination_with_sort_columns(self.sortable_columns, self.default_sort_column)
        request_data = TinyShield(model).block(self.request.query_params)
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", self.default_sort_column),
            sort_order=request_data["order"],
        )
