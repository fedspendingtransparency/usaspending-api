import numpy
from django.db.models import Q, Sum, Count, Max, Min, FloatField
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.search.models import TransactionSearch


class RecipientList(AgencyBase):
    """
    Obtain the count of recipients for a specific agency in a single
    fiscal year as well as the percentile data needed for a box-and-whisker plot.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/recipients.md"

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        results = self.get_recipients_queryset()
        return Response(results)

    def get_recipients_queryset(self):
        SQL="""
        with recipient_cte as (
            select funding_agency_id, recipient_hash, sum(generated_pragmatic_obligation) as recipient_amount
            from transaction_search
            where fiscal_year={fiscal_year} and
                funding_agency_id={agency_id} and
                action_date >= '2007-10-01' and
                recipient_hash is not null
            group by funding_agency_id, recipient_hash
        )
        select
        count(recipient_hash),
        max(recipient_amount),
        min(recipient_amount),
        percentile_disc(0.25) within group (order by recipient_cte.recipient_amount) as "25th_percentile",
        percentile_disc(0.5) within group (order by recipient_cte.recipient_amount) as "50th_percentile",
        percentile_disc(0.75) within group (order by recipient_cte.recipient_amount) as "75th_percentile"
        from recipient_cte group by funding_agency_id;
        """.format(fiscal_year=self.fiscal_year, agency_id=self.agency_id)

        results = execute_sql_to_ordered_dictionary(SQL)
        return {
            "toptier_code": self.toptier_code,
            "fiscal_year": self.fiscal_year,
            "results": results,
            "messages": self.standard_response_messages
        }
        return results
        # results = (
        #     TransactionSearch.objects.filter(
        #         funding_agency_id=self.agency_id,
        #         fiscal_year=self.fiscal_year,
        #         recipient_unique_id__isnull=False,
        #         action_date__gte="2007-10-01",
        #     )
        #     .values("recipient_unique_id")
        #     .annotate(recipient_value=Sum("generated_pragmatic_obligation", output_field=FloatField()))
        #     .values_list("recipient_value", flat=True)
        # )
        # if len(results) > 0:
        #     count = len(results)
        #     recipient_max = max(results)
        #     recipient_min = min(results)
        #     numpy.amax(count)
        #     percentile_25 = numpy.percentile(results, 25)
        #     percentile_50 = numpy.percentile(results, 50)
        #     percentile_75 = numpy.percentile(results, 75)
        #     formatted_results = {
        #         "toptier_code": self.toptier_code,
        #         "fiscal_year": self.fiscal_year,
        #         "count": count,
        #         "max": recipient_max,
        #         "min": recipient_min,
        #         "25th_percentile": round(percentile_25, 2),
        #         "50th_percentile": round(percentile_50, 2),
        #         "75th_percentile": round(percentile_75, 2),
        #         "messages": self.standard_response_messages,
        #     }
        # else:
        #     formatted_results = {
        #         "toptier_code": self.toptier_code,
        #         "fiscal_year": self.fiscal_year,
        #         "count": 0,
        #         "max": None,
        #         "min": None,
        #         "25th_percentile": None,
        #         "50th_percentile": None,
        #         "75th_percentile": None,
        #         "messages": self.standard_response_messages,
        #     }
        # return formatted_results
