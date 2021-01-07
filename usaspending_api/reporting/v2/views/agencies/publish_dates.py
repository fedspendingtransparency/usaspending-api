import re
from builtins import next, enumerate
from copy import deepcopy

from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import OuterRef, Subquery, TextField, F, Value, Func, DecimalField, Q
from django.db.models.functions import Cast
from django.utils.functional import cached_property
from jsonpickle import json
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin

from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import ToptierAgency
from usaspending_api.reporting.models import ReportingAgencyOverview
from usaspending_api.submissions.models import SubmissionAttributes


class PublishDates(AgencyBase, PaginationMixin):
    """Returns list of agency submission information, included published and certified dates for the fiscal year"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/publish_dates.md"

    def validate_publication_sort(self, sort_key):
        regex_string = r"publication_date,([2-9]|1[0-2])"
        if not re.match(regex_string, sort_key):
            raise UnprocessableEntityException(
                "publication_date sort param must be in the format 'publication_date,<fiscal_period>'"
            )

    def get_agency_data(self):
        agency_filters = [Q(toptier_code=OuterRef("toptier_code"))]
        if self.filter is not None:
            agency_filters.append(Q(name__icontains=self.filter))
        results = (
            SubmissionAttributes.objects.filter(reporting_fiscal_year=self.fiscal_year)
            .values("toptier_code")
            .annotate(
                agency_name=Subquery(ToptierAgency.objects.filter(*agency_filters).values("name")),
                abbreviation=Subquery(ToptierAgency.objects.filter(*agency_filters).values("abbreviation")),
                current_total_budget_authority_amount=Subquery(
                    ReportingAgencyOverview.objects.filter(
                        toptier_code=OuterRef("toptier_code"), fiscal_year=OuterRef("reporting_fiscal_year")
                    )
                    .annotate(the_sum=Func(F("total_budgetary_resources"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                periods=ArrayAgg(
                    ConcatAll(
                        Value('{"reporting_fiscal_period": "'),
                        Cast("reporting_fiscal_period", output_field=TextField()),
                        Value('", "reporting_fiscal_quarter": "'),
                        Cast("reporting_fiscal_quarter", output_field=TextField()),
                        Value('", "submission_dates":{ "publication_date": "'),
                        Cast("published_date", output_field=TextField()),
                        Value('", "certification_date": "'),
                        Cast("certified_date", output_field=TextField()),
                        Value('"}, "quarterly": "'),
                        Cast("quarter_format_flag", output_field=TextField()),
                        Value('"}'),
                    )
                ),
            )
            .exclude(agency_name__isnull=True)
            .values("agency_name", "abbreviation", "toptier_code", "current_total_budget_authority_amount", "periods")
        )
        return self.format_results(results)

    def format_results(self, result_list):
        results = []
        for result in result_list:
            results.append(
                {
                    "agency_name": result["agency_name"],
                    "abbreviation": result["abbreviation"],
                    "agency_code": result["toptier_code"],
                    "current_total_budget_authority_amount": result["current_total_budget_authority_amount"],
                    "periods": sorted(
                        [json.loads(x) for x in result["periods"]], key=lambda x: x["reporting_fiscal_period"]
                    )
                    if result.get("periods")
                    else None,
                }
            )
        return results

    def get(self, request):
        if "publication_date" in self.pagination.sort_key:
            self.validate_publication_sort(self.pagination.sort_key)
            sort_key = deepcopy(self.pagination.sort_key)
            pub_sort = sort_key.split(",")[1]
            self.pagination.sort_key = "publication_date"
            results = sorted(
                self.get_agency_data(),
                key=lambda x: x["periods"][
                    next(
                        (index for (index, d) in enumerate(x["periods"]) if d["reporting_fiscal_period"] == pub_sort), 0
                    )
                ]["submission_dates"]["publication_date"],
                reverse=(self.pagination.sort_order == "desc"),
            )
        else:
            results = sorted(
                self.get_agency_data(),
                key=lambda x: x[self.pagination.sort_key],
                reverse=(self.pagination.sort_order == "desc"),
            )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )

    @cached_property
    def pagination(self):
        sortable_columns = [
            "agency_name",
            "abbreviation",
            "agency_code",
            "current_total_budget_authority_amount",
            "publication_date",
        ]
        default_sort_column = "current_total_budget_authority_amount"
        models = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        models.extend([{"key": "fiscal_year", "name": "fiscal_year", "type": "integer", "optional": False}])
        if self.request.query_params.get("sort") and "publication_date" in self.request.query_params.get("sort"):
            modified_query_params = deepcopy(self.request.query_params)
            modified_query_params.pop("sort")
            request_data = TinyShield(models).block(modified_query_params)
            request_data["sort"] = self.request.query_params.get("sort")
        else:
            request_data = TinyShield(models).block(self.request.query_params)
        # since publication_date requires a variable that we can't check for using the enum check, we're doing it seperately
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", default_sort_column),
            sort_order=request_data["order"],
        )
