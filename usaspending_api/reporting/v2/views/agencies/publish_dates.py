import re
from copy import deepcopy

from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import OuterRef, Subquery, TextField, Value, Q, Max
from django.db.models.functions import Cast
from django.utils.functional import cached_property
import json
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.fiscal_year_helpers import get_quarter_from_period
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import ToptierAgencyPublishedDABSView
from usaspending_api.reporting.models import ReportingAgencyOverview
from usaspending_api.submissions.helpers import is_valid_monthly_period
from usaspending_api.submissions.models import SubmissionAttributes


class PublishDates(PaginationMixin, AgencyBase):
    """Returns list of agency submission information, included published and certified dates for the fiscal year"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/publish_dates.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "filter"]

    def validate_publication_sort(self, sort_key):
        regex_string = r"publication_date,([2-9]|1[0-2])"
        if not re.match(regex_string, sort_key):
            raise UnprocessableEntityException(
                "publication_date sort param must be in the format 'publication_date,<fiscal_period>' where <fiscal_period> is in the range 2-12"
            )

    def get_agency_data(self):

        agency_filters = []
        if self.filter is not None:
            agency_filters.append(Q(name__icontains=self.filter) | Q(abbreviation__icontains=self.filter))

        results = (
            ToptierAgencyPublishedDABSView.objects.annotate(
                fiscal_period=Subquery(
                    SubmissionAttributes.objects.filter(
                        submission_window__submission_fiscal_year=self.fiscal_year,
                        submission_window__submission_reveal_date__lte=now(),
                        toptier_code=OuterRef("toptier_code"),
                    )
                    .values("toptier_code", "submission_window__submission_fiscal_year")
                    .annotate(fiscal_period=Max("submission_window__submission_fiscal_month"))
                    .values("fiscal_period")
                )
            )
            .filter(*agency_filters)
            .annotate(
                current_total_budget_authority_amount=Subquery(
                    ReportingAgencyOverview.objects.filter(
                        toptier_code=OuterRef("toptier_code"),
                        fiscal_year=self.fiscal_year,
                        fiscal_period=OuterRef("fiscal_period"),
                    ).values("total_budgetary_resources")
                ),
                periods=Subquery(
                    SubmissionAttributes.objects.filter(
                        reporting_fiscal_year=self.fiscal_year,
                        toptier_code=OuterRef("toptier_code"),
                        submission_window__submission_reveal_date__lte=now(),
                    )
                    .values("toptier_code")
                    .annotate(
                        period=ArrayAgg(
                            ConcatAll(
                                Value('{"period": '),
                                Cast("reporting_fiscal_period", output_field=TextField()),
                                Value(', "quarter": '),
                                Cast("reporting_fiscal_quarter", output_field=TextField()),
                                Value(', "submission_dates":{ "publication_date": "'),
                                Cast("published_date", output_field=TextField()),
                                Value('", "certification_date": "'),
                                Cast("certified_date", output_field=TextField()),
                                Value('"}, "quarterly": '),
                                Cast("quarter_format_flag", output_field=TextField()),
                                Value("}"),
                                output_field=TextField(),
                            )
                        )
                    )
                    .values("period"),
                    output_field=TextField(),
                ),
            )
            .values("name", "toptier_code", "abbreviation", "current_total_budget_authority_amount", "periods")
        )
        return self.format_results(results)

    def format_results(self, result_list):
        results = []
        for result in result_list:
            periods = [json.loads(x) for x in result["periods"]] if result.get("periods") else []
            existing_periods = set([x["period"] for x in periods])
            missing_periods = set({2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}).difference(existing_periods)
            for x in missing_periods:
                # cannot always infer if the missing periods for 3, 6, 9, 12 should/would have been
                # submitted as a quarterly, so defaulting to monthly for consistency
                periods.append(
                    {
                        "period": x,
                        "quarter": get_quarter_from_period(x),
                        "submission_dates": {"publication_date": "", "certification_date": ""},
                        "quarterly": False,
                    }
                )
            periods = filter(lambda period: period["period"] in self.displayed_periods, periods)
            results.append(
                {
                    "agency_name": result["name"],
                    "abbreviation": result["abbreviation"],
                    "toptier_code": result["toptier_code"],
                    "current_total_budget_authority_amount": result["current_total_budget_authority_amount"],
                    "periods": sorted(periods, key=lambda x: x["period"]),
                }
            )
        return results

    def get(self, request):
        self.displayed_periods = list(
            filter(lambda period: is_valid_monthly_period(self.fiscal_year, period), list(range(2, 13)))
        )
        if "publication_date" in self.pagination.sort_key:
            self.validate_publication_sort(self.pagination.sort_key)
            sort_key = deepcopy(self.pagination.sort_key)
            period_param = int(sort_key.split(",")[1])
            try:
                pub_sort = self.displayed_periods.index(period_param)
            except ValueError:
                pub_sort = None
            self.pagination.sort_key = "publication_date"

            # If not a valid period for the fiscal year fallback to the agency name
            results = sorted(
                self.get_agency_data(),
                key=lambda x: (
                    x["periods"][pub_sort]["submission_dates"]["publication_date"]
                    if pub_sort is not None
                    else x["agency_name"]
                ),
                reverse=(self.pagination.sort_order == "desc"),
            )
        else:
            results = sorted(
                self.get_agency_data(),
                key=lambda x: (
                    (x[self.pagination.sort_key] is None) == (self.pagination.sort_order == "asc"),
                    x[self.pagination.sort_key],
                ),
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
            "toptier_code",
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
        # since publication_date requires a variable that we can't check for using the enum check, we're doing it separately
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", default_sort_column),
            sort_order=request_data["order"],
        )
