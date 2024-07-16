from copy import deepcopy

from django.db.models import Sum
from elasticsearch_dsl import A
from elasticsearch_dsl import Q as ES_Q
from rest_framework.response import Response

from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    loan_type_mapping,
    procurement_type_mapping,
)
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import AccountSearch
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.disaster.v2.views.disaster_base import AwardTypeMixin, DisasterBase, FabaOutlayMixin
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float


class AmountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """Returns aggregated values of obligation, outlay, and count of Award records"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"
    count_only = False

    @cache_response()
    def post(self, request):
        additional_models = [
            {
                "key": "filter|award_type",
                "name": "award_type",
                "type": "enum",
                "enum_values": ("assistance", "procurement"),
                "allow_nulls": False,
                "optional": True,
            }
        ]

        f = TinyShield(additional_models).block(self.request.data).get("filter")
        if f:
            self.filters["award_type"] = f.get("award_type")

        if all(x in self.filters for x in ["award_type_codes", "award_type"]):
            raise UnprocessableEntityException("Cannot provide both 'award_type_codes' and 'award_type'")

        award_type = self.filters.get("award_type")

        queryset = (
            CovidFABASpending.objects.filter(spending_level="awards")
            .filter(defc__in=self.filters["def_codes"])
            .annotate(
                total_award_count=Sum("award_count"),
                total_obligation_sum=Sum("obligation_sum"),
                total_outlay_sum=Sum("outlay_sum"),
                total_face_value_of_loan=Sum("face_value_of_loan"),
            )
        )

        if self.award_type_codes:
            queryset = queryset.filter(award_type__in=self.award_type_codes)

        if award_type and award_type.lower() == "procurement":
            queryset = queryset.filter(award_type__in=procurement_type_mapping.keys())
        elif award_type and award_type.lower() == "assistance":
            queryset = queryset.filter(award_type__in=assistance_type_mapping.keys())

        result = {
            "award_count": sum([row.total_award_count for row in queryset]),
            "obligation": sum([row.total_obligation_sum for row in queryset]),
            "outlay": sum([row.total_outlay_sum for row in queryset]),
        }

        # Add face_value_of_loan if any loan award types were included in the request
        if self.award_type_codes and any(
            award_type in loan_type_mapping.keys() for award_type in self.award_type_codes
        ):
            result["face_value_of_loan"] = sum(
                [row.total_face_value_of_loan for row in queryset if row.total_face_value_of_loan is not None]
            )

        if self.count_only:
            return Response({"count": result["award_count"]})
        else:
            return Response(result)

    def build_elasticsearch_search(self) -> AccountSearch:
        if self.award_type_codes:
            count_field = "award_id"
        else:
            count_field = "financial_account_distinct_award_key"

        filter_query = self._build_elasticsearch_query()
        search = AccountSearch().filter(filter_query)

        count_agg = create_count_aggregation(count_field)

        financial_accounts_agg = A("nested", path="financial_accounts_by_award")
        filter_agg_query = ES_Q(
            "terms", **{"financial_accounts_by_award.disaster_emergency_fund_code": self.filters.get("def_codes")}
        )
        filtered_aggs = A("filter", filter_agg_query)
        outlay_sum_agg = A(
            "sum",
            script="""doc['financial_accounts_by_award.is_final_balances_for_fy'].value ? (
                (doc['financial_accounts_by_award.gross_outlay_amount_by_award_cpe'].size() > 0 ? doc['financial_accounts_by_award.gross_outlay_amount_by_award_cpe'].value : 0)
                + (doc['financial_accounts_by_award.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].size() > 0 ? doc['financial_accounts_by_award.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].value : 0)
                + (doc['financial_accounts_by_award.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].size() > 0 ? doc['financial_accounts_by_award.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].value : 0)
            ) * 100 : 0""",
        )
        obligation_sum_agg = A(
            "sum", field="financial_accounts_by_award.transaction_obligated_amount", script="_value * 100"
        )
        reverse_nested_agg = A("reverse_nested", **{})
        face_value_of_loan_sum_agg = A("sum", field="total_loan_value", script="_value * 100")

        search.aggs.bucket("nested_agg", financial_accounts_agg).bucket("filter_agg", filtered_aggs).metric(
            "obligation_sum_agg", obligation_sum_agg
        ).metric("outlay_sum_agg", outlay_sum_agg).bucket("reverse_nested_agg", reverse_nested_agg).metric(
            "count_agg", count_agg
        ).metric(
            "face_value_of_loan_sum_agg", face_value_of_loan_sum_agg
        )

        return search

    def _build_elasticsearch_query(self) -> ES_Q:
        filters = deepcopy(self.filters)
        award_type_filter = filters.pop("award_type", None)
        filters["nonzero_fields"] = self.nonzero_fields

        if filters.get("def_codes"):
            filters["nested_def_codes"] = filters.pop("def_codes")

        filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(filters)

        if award_type_filter:
            is_procurement = award_type_filter == "procurement"
            exists_query = ES_Q("exists", field="financial_accounts_by_award.piid")
            nested_query = ES_Q(
                "nested",
                path="financial_accounts_by_award",
                query=ES_Q("bool", **{f"must{'' if is_procurement else '_not'}": exists_query}),
            )
            filter_query.must.append(nested_query)

        return filter_query

    def build_result(self, search: AccountSearch) -> dict:
        response = search.handle_execute()
        response = response.aggs.to_dict()
        filter_agg = response.get("nested_agg", {}).get("filter_agg", {})
        reverse_nested_agg = filter_agg.get("reverse_nested_agg", {})

        result = {
            "award_count": reverse_nested_agg.get("count_agg", {"value": 0})["value"],
            "obligation": get_summed_value_as_float(filter_agg, "obligation_sum_agg"),
            "outlay": get_summed_value_as_float(filter_agg, "outlay_sum_agg"),
        }

        if "total_loan_value" in self.nonzero_fields:
            result["face_value_of_loan"] = get_summed_value_as_float(reverse_nested_agg, "face_value_of_loan_sum_agg")

        return result
