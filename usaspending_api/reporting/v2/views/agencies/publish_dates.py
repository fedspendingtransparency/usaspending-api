from django.contrib.postgres.aggregates import ArrayAgg, JSONBAgg
from django.contrib.postgres.fields import JSONField
from django.db.models import OuterRef, Subquery, DateTimeField, TextField, F, Value, Func, DecimalField
from django.db.models.functions import Concat, Cast
from jsonpickle import json
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin

from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.references.models import ToptierAgency
from usaspending_api.reporting.models import ReportingAgencyOverview
from usaspending_api.submissions.models import SubmissionAttributes


class PublishDates(AgencyBase, PaginationMixin):
    """Placeholder"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/publish_dates.md"

    # {
    #     "agency_name": "Department of Health and Human Services",
    #     "abbreviation": "DHHS",
    #     "agency_code": "020",
    #     "current_total_budget_authority_amount": 8361447130497.72,
    #     "periods": [{
    #         "period": 2,
    #         "quarter": 1,
    #         "submission_dates": {
    #             "publication_date": "2020-01-20T11:59:21Z",
    #             "certification_date": "2020-01-21T10:58:21Z"
    #         },
    #         "quarterly": false
    #     }]
    # },
    def get_agency_data(self):
        results = (
            SubmissionAttributes.objects.filter(reporting_fiscal_year=self.fiscal_year)
            .annotate(
                agency_name=Subquery(
                    ToptierAgency.objects.filter(toptier_code=OuterRef("toptier_code")).values("name")
                ),
                abbreviation=Subquery(
                    ToptierAgency.objects.filter(toptier_code=OuterRef("toptier_code")).values("abbreviation")
                ),
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
                        Value('{"publication_date": "'),
                        Cast("published_date", output_field=TextField()),
                        Value('", "certification_date": "'),
                        Cast("certified_date", output_field=TextField()),
                        Value('", "submission_dates": {"reporting_fiscal_period": "'),
                        Cast("reporting_fiscal_period", output_field=TextField()),
                        Value('", "reporting_fiscal_quarter": "'),
                        Cast("reporting_fiscal_quarter", output_field=TextField()),
                        Value('"}, "quarterly": "'),
                        Cast("quarter_format_flag", output_field=TextField()),
                        Value('"}'),
                    )
                ),
            )
            .values("agency_name", "abbreviation", "toptier_code", "current_total_budget_authority_amount", "periods")
            .order_by("toptier_code")
        )
        # ReportingAgencyOverview.objects.filter(fiscal_year=self.fiscal_year)
        # .annotate(
        #     agency_name=Subquery(
        #         ToptierAgency.objects.filter(toptier_code=OuterRef("toptier_code")).values("name")
        #     ),
        #     abbreviation=Subquery(
        #         ToptierAgency.objects.filter(toptier_code=OuterRef("toptier_code")).values("abbreviation")
        #     ),
        #     periods=Subquery(
        #         SubmissionAttributes.objects.filter(
        #             reporting_fiscal_year=OuterRef("fiscal_year"), toptier_code=OuterRef("toptier_code")
        #         )
        #         .annotate(
        #             json=ConcatAll(
        #                 Value('{"publication_date": "'), Cast("published_date", output_field=TextField()),
        #                 Value('", "certification_date": "'), Cast("certified_date", output_field=TextField()),
        #                 Value('", "reporting_fiscal_period": "'), Cast("reporting_fiscal_period", output_field=TextField()),
        #                 Value('", "reporting_fiscal_quarter": "'), Cast("reporting_fiscal_quarter", output_field=TextField()),
        #                 Value('", "quarter_format_flag": "'),  Cast("quarter_format_flag", output_field=TextField()),
        #                 Value('"}')
        #             )
        #         )
        #         .values("toptier_code").annotate(arr=ArrayAgg(F("json"))).values("arr"), output_field=TextField()
        #     ),
        # ).values("agency_name", "abbreviation", "toptier_code", "current_total_budget_authority_amount").order_by("toptier_code")
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
                    "periods": [json.loads(x) for x in result["periods"]] if result.get("periods") else None,
                }
            )
        return results

    def get(self, request):
        self.sortable_columns = [
            "agency_name",
            "abbreviation",
            "toptier_code",
            "current_total_budget_authority_amount",
            "publication_date",
        ]
        self.default_sort_column = "current_total_budget_authority_amount"
        results = self.get_agency_data()
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {"page_metadata": page_metadata, "results": results, "messages": self.standard_response_messages}
        )
