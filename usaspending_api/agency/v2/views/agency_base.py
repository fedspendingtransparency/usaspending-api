from django.conf import settings
from django.utils.functional import cached_property
from re import fullmatch
from rest_framework.exceptions import NotFound
from rest_framework.views import APIView

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.common.helpers.generic_helper import get_account_data_time_period_message
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns
from usaspending_api.references.models import ToptierAgency


class AgencyBase(APIView):
    @property
    def toptier_code(self):
        # We don't have to do any validation here because Django has already checked this to be
        # either a three or four digit numeric string based on the regex pattern in our route url.
        return self.kwargs["toptier_code"]

    @cached_property
    def fiscal_year(self):
        if self.request.method == "GET":
            fiscal_year = self.request.query_params.get("fiscal_year")
        else:
            fiscal_year = self.request.data.get("fiscal_year")
        fiscal_year = str(fiscal_year or current_fiscal_year())
        if not fullmatch("[0-9]{4}", fiscal_year):
            raise UnprocessableEntityException("Unrecognized fiscal_year format. Should be YYYY.")
        min_fiscal_year = fy(settings.API_SEARCH_MIN_DATE)
        fiscal_year = int(fiscal_year)
        if fiscal_year < min_fiscal_year:
            raise UnprocessableEntityException(
                f"fiscal_year is currently limited to an earliest year of {min_fiscal_year}."
            )
        if fiscal_year > current_fiscal_year():
            raise UnprocessableEntityException(
                f"fiscal_year may not exceed current fiscal year of {current_fiscal_year()}."
            )
        return fiscal_year

    @cached_property
    def toptier_agency(self):
        toptier_agency = ToptierAgency.objects.account_agencies().filter(toptier_code=self.toptier_code).first()
        if not toptier_agency:
            raise NotFound(f"Agency with a toptier code of '{self.toptier_code}' does not exist")
        return toptier_agency

    @property
    def standard_response_messages(self):
        return [get_account_data_time_period_message()] if self.fiscal_year < 2017 else []

    @cached_property
    def pagination(self):
        if self.request.method == "GET":
            return None
        sortable_columns = ["name", "obligated_amount", "gross_outlay_amount"]
        default_sort_column = "obligated_amount"
        model = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.data)
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]) + 1,
            sort_key=request_data.get("sort", "obligated_amount"),
            sort_order=request_data["order"],
        )

    @property
    def filter(self):
        if self.request.method == "GET":
            return None
        return self.request.data.get("filter")
