import copy
import logging

from django.conf import settings
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models.aggregates import Aggregate
from django.db.models import Func, IntegerField
from rest_framework.response import Response
from rest_framework.views import APIView

# from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.view_selector import new_awards_summary
# from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.tinyshield import TinyShield
# from usaspending_api.settings import API_MAX_DATE, API_SEARCH_MIN_DATE
from usaspending_api.recipient.models import RecipientProfile

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


class ArrayCat(Aggregate):
    function = 'ARRAY_CAT'

    def convert_value(self, value, expression, connection, context):
        if not value:
            return []
        return value


class Month(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from %(expressions)s)"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) - INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) - INTERVAL '3 months')"
    output_field = IntegerField()


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class NewAwardsOverTimeVisualizationViewSet(APIView):
    """

    endpoint_doc: /advanced_award_search/spending_over_time.md
    """

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        groupings = {"quarter": "q", "q": "q", "fiscal_year": "fy", "fy": "fy", "month": "m", "m": "m"}
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {"name": "group", "key": "group", "type": "enum", "enum_values": list(groupings.keys()), "default": "fy"},
            {
                "name": "recipient_id",
                "key": "filters|recipient_id",
                "type": "text",
                "text_type": "search",
                "optional": False,
            },
        ]
        advanced_search_filters = [
            # add "recipient_id" once another PR is merged
            model
            for model in copy.deepcopy(AWARD_FILTER)
            if model["name"] in ("time_period", "award_type_codes")
        ]
        models.extend(advanced_search_filters)
        json_request = TinyShield(models).block(request.data)
        filters = json_request.get("filters", None)

        if filters is None:
            raise InvalidParameterException("Missing request parameters: filters")

        if json_request["subawards"]:
            raise NotImplementedError("subawards are not implemented yet")

        recipient_hash = filters["recipient_id"][:-2]
        is_parent = True if filters["recipient_id"][-1] == 'P' else False

        queryset, model = new_awards_summary(filters)

        if is_parent:
            # there is only one record with that hash and recipient_level = 'P'
            parent_duns_rows = (RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='P')
                .values('recipient_unique_id')
            )
            if len(parent_duns_rows) != 1:
                raise InvalidParameterException('Provided recipient_id has no parent records')
            parent_duns = parent_duns_rows[0]['recipient_unique_id']
            queryset = queryset.filter(parent_recipient_unique_id=parent_duns)
        else:
            queryset = queryset.filter(recipient_hash=recipient_hash)

        values = ["year"]
        if groupings[json_request["group"]] == "m":
            queryset = queryset.annotate(month=Month("action_date"), year=FiscalYear("action_date"))
            values.append("month")

        elif groupings[json_request["group"]] == "q":
            queryset = queryset.annotate(quarter=FiscalQuarter("action_date"), year=FiscalYear("action_date"))
            values.append("quarter")

        elif groupings[json_request["group"]] == "fy":
            queryset = queryset.annotate(year=FiscalYear("action_date"))

        if model == 'UniversalTransactionView':
            queryset = (
                queryset.values(*values)
                .annotate(award_ids=ArrayAgg("award_id"))
                .order_by(*["-{}".format(value) for value in values])
            )
        else:
            queryset = (
                queryset.values(*values)
                .annotate(award_ids=ArrayCat("award_ids"))
                .order_by(*["-{}".format(value) for value in values])
            )

        from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
        print('=======================================')
        print(request.path)
        print(generate_raw_quoted_query(queryset))

        results = []
        previous_awards = set()
        for row in queryset:
            new_awards = set(row["award_ids"]) - previous_awards
            result = {
                "time_period": {},
                "award_ids_in_period": set(row["award_ids"]),
                "transaction_count_in_period": len(row["award_ids"]),
                "new_award_count_in_period": len(new_awards),
            }
            for period in values:
                result["time_period"]["fiscal_{}".format(period)] = row[period]
            results.append(result)
            previous_awards.update(row["award_ids"])
        response_dict = {"group": groupings[json_request["group"]], "results": results}

        return Response(response_dict)
