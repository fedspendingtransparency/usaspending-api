import copy
import logging

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.query_types import TransactionSearchTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod

logger = logging.getLogger(__name__)


class DownloadTransactionCountViewSet(APIView):
    """
    Returns the number of transactions that would be included in a download request for the given filter set.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/count.md"

    def deprecation_message1(self):
        return (
            "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
            "See documentation for more information. "
        )

    def deprecation_message2(self):
        return (
            "The above fields containing the transaction_* naming convention "
            "will be deprecated and replaced with fields without the transaction_*. "
        )

    @cache_response()
    def post(self, request):
        """Returns boolean of whether a download request is greater than the max limit."""
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": ["awards", "transactions", "subawards"],
                "optional": True,
                "default": "transactions",
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        self.original_filters = request.data.get("filters")
        json_request = TinyShield(models).block(request.data)

        # If no filters in request return empty object to return all transactions
        filters = json_request.get("filters", {})

        # 'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead.
        # See documentation for more information.
        if json_request["subawards"] or json_request["spending_level"] == "subawards":
            total_count = subaward_filter(filters).count()
        elif json_request["spending_level"] == "transactions":
            options = {}
            time_period_obj = TransactionSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
            )
            new_awards_only_decorator = NewAwardsOnlyTimePeriod(
                time_period_obj=time_period_obj, query_type=QueryType.TRANSACTIONS
            )
            options["time_period_obj"] = new_awards_only_decorator
            query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
            filter_query = query_with_filters.generate_elasticsearch_query(filters, **options)
            search = TransactionSearch().filter(filter_query)
            total_count = search.handle_count()
        else:
            options = {}
            time_period_obj = AwardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
            )
            new_awards_only_decorator = NewAwardsOnlyTimePeriod(
                time_period_obj=time_period_obj, query_type=QueryType.AWARDS
            )
            options["time_period_obj"] = new_awards_only_decorator
            query_with_filters = QueryWithFilters(QueryType.AWARDS)
            filter_query = query_with_filters.generate_elasticsearch_query(filters, **options)
            search = AwardSearch().filter(filter_query)
            total_count = search.handle_count()

        if total_count is None:
            total_count = 0

        message_list = get_generic_filters_message(
            self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]
        )
        message_list.append(self.deprecation_message1())
        message_list.append(self.deprecation_message2())

        result = {
            # TODO: the following fields that follow the transaction_ naming convention will be removed
            # once the front end is ready to implement the new fields.
            "calculated_transaction_count": total_count,
            "maximum_transaction_limit": settings.MAX_DOWNLOAD_LIMIT,
            "transaction_rows_gt_limit": total_count > settings.MAX_DOWNLOAD_LIMIT,
            "calculated_count": total_count,
            "spending_level": json_request["spending_level"],
            "maximum_limit": settings.MAX_DOWNLOAD_LIMIT,
            "rows_gt_limit": total_count > settings.MAX_DOWNLOAD_LIMIT,
            "messages": message_list,
        }

        return Response(result)
