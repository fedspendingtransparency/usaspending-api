import copy
import logging
from fiscalyear import FiscalDate

from django.conf import settings
from django.contrib.postgres.aggregates.general import ArrayAgg, StringAgg
from django.db.models import Func, IntegerField, Sum
from django.db.models.aggregates import Aggregate
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.settings import API_MAX_DATE, API_SEARCH_MIN_DATE
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.awards.models_matviews import SummaryAwardRecipientView
from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


class ArrayCat(Aggregate):
    function = 'ARRAY_CAT'

    def convert_value(self, value, expression, connection, context):
        if not value:
            return []
        return value


class FiscalMonth(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class NewAwardsOverTimeVisualizationViewSet(APIView):
    """

    endpoint_doc: /advanced_award_search/spending_over_time.md
    """

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        groupings = {
            "quarter": "quarter", "q": "quarter",
            "fiscal_year": "fiscal_year", "fy": "fiscal_year",
            "month": "month", "m": "month"}
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {"name": "group", "key": "group", "type": "enum", "enum_values": list(groupings.keys()), "default": "fy"},
        ]
        advanced_search_filters = [
            model for model in copy.deepcopy(AWARD_FILTER)
            if model["name"] in ("time_period", "award_type_codes", "recipient_id")
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

        queryset = SummaryAwardRecipientView.objects.filter()

        queryset &= combine_date_range_queryset(filter['time_period'], SummaryAwardRecipientView, API_SEARCH_MIN_DATE, API_MAX_DATE)

        if is_parent:
            # there *should* only one record with that hash and recipient_level = 'P'
            parent_duns_rows = (
                RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='P')
                .values('recipient_unique_id')
            )
            if len(parent_duns_rows) != 1:
                raise InvalidParameterException('Provided recipient_id has no parent records')
            parent_duns = parent_duns_rows[0]['recipient_unique_id']
            queryset = queryset.filter(parent_recipient_unique_id=parent_duns)
        else:
            queryset = queryset.filter(recipient_hash=recipient_hash)

        values = ["year"]
        if groupings[json_request["group"]] == "month":
            queryset = queryset.annotate(month=FiscalMonth("date_signed"), year=FiscalYear("date_signed"))
            values.append("month")

        elif groupings[json_request["group"]] == "quarter":
            queryset = queryset.annotate(quarter=FiscalQuarter("date_signed"), year=FiscalYear("date_signed"))
            values.append("quarter")

        elif groupings[json_request["group"]] == "fiscal_year":
            queryset = queryset.annotate(year=FiscalYear("date_signed"))

        queryset = (
            queryset.values(*values)
            .annotate(counts=Sum('count'))
            .order_by(*["-{}".format(value) for value in values])
        )

        from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
        print('=======================================')
        print(request.path)
        print(generate_raw_quoted_query(queryset))

        results = []
        previous_awards = set()
        from time import perf_counter
        start = perf_counter()
        for row in queryset:
            result = {
                "time_period": {}
            }
            for period in values:
                result["time_period"]["fiscal_{}".format(period)] = row[period]

            # all_dates_signed = [FiscalDate(awd_dt.year, awd_dt.month, awd_dt.day) for awd_dt in row["award_creations"]]
            # new_award_count = 0
            # for d in all_dates_signed:
            #     # print(f'Fiscal Year: {d.year} Fiscal Quarter: {d.quarter}')
            #     # print(row['year'], row['quarter'])
            #     if d.year == row['year']:
            #         if 'quarter' in values:
            #             if d.quarter == row['quarter']:
            #                 new_award_count += 1
            #         elif 'month' in values:
            #             if d.month == row['month']:
            #                 new_award_count += 1
            #         else:
            #             new_award_count += 1

            # awards = [int(x) for x in row["award_ids"].split(' ')]
            # new_awards = set(awards) - previous_awards

            # result["award_ids_in_period"] = set(awards)
            result["new_award_count_in_period"] = row['counts']
            # result["transaction_count_in_period"] = row["transaction_count"]
            results.append(result)
            # previous_awards.update(new_awards)
        end = perf_counter()
        print(f"bl took {end-start}s")
        response_dict = {"group": groupings[json_request["group"]], "results": results}

        return Response(response_dict)
