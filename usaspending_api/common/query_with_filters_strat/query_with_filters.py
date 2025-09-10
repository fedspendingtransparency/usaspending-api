from usaspending_api.common.query_with_filters_strat.award import AwardStrategy
from usaspending_api.common.query_with_filters_strat.keywords import KeywordStrategy
from usaspending_api.common.query_with_filters_strat.program import ProgramStrategy
from usaspending_api.common.query_with_filters_strat.type_codes import TypeCodesStrategy
from usaspending_api.search.filters.elasticsearch.filter import QueryType, _Filter
import copy
import itertools
import logging
from django.conf import settings
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import (
    DUPLICATE_DISTRICT_LOCATION_PARAMETERS,
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
)
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.references.models.psc import PSC
from usaspending_api.search.filters.elasticsearch.filter import QueryType, _Filter
from usaspending_api.search.filters.elasticsearch.naics import NaicsCodes
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes
from usaspending_api.search.filters.elasticsearch.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import (
    AwardSearchTimePeriod,
    SubawardSearchTimePeriod,
    TransactionSearchTimePeriod,
)
from usaspending_api.common.query_with_filters_strat.recipient_pop import RecipientPOPStrategy
from usaspending_api.common.query_with_filters_strat.jobs import QueryWithFiltersJobs, SubawardStrategy, AwardsStrategy, TransactionStrategy
from usaspending_api.common.query_with_filters_strat.misc_strategies import MiscStrategy
from usaspending_api.search.v2.es_sanitization import es_sanitize
logger = logging.getLogger(__name__)
class QueryWithFilters:

    strategy_map = {
        "recipient": RecipientPOPStrategy,
        "place_of_performance": RecipientPOPStrategy,
        "type_codes": TypeCodesStrategy,
        "keyword": KeywordStrategy,
        "program": ProgramStrategy,
        "award": AwardStrategy,
    }

    nested_filter_lookup = {

    }

    unsupported_filters = ["legal_entities"]

    def __init__(self, query_type: QueryType):
        self.query_type = query_type
        time_period_obj = None
        if self.query_type == QueryType.ACCOUNTS:
            self.default_options = {"nested_path": "financial_accounts_by_award"}
        elif self.query_type == QueryType.TRANSACTIONS:
            time_period_obj = TransactionSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )
        elif self.query_type == QueryType.AWARDS:
            time_period_obj = AwardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )
        elif self.query_type == QueryType.SUBAWARDS:
            time_period_obj = SubawardSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_MIN_DATE
            )

            self.default_options = {"time_period_obj": time_period_obj}

        if time_period_obj is not None and (
            self.query_type == QueryType.AWARDS or self.query_type == QueryType.TRANSACTIONS
        ):
            new_awards_only_decorator = NewAwardsOnlyTimePeriod(
                time_period_obj=time_period_obj, query_type=self.query_type
            )
            self.default_options = {"time_period_obj": new_awards_only_decorator}

    def generate_elasticsearch_query(self, filters: dict, **options) -> ES_Q:
        options = {**self.default_options, **options}
        nested_path = options.pop("nested_path", "")
        must_queries = []
        nested_must_queries = []

        # Create a copy of the filters so that manipulating the filters for the purpose of building the ES query
        # does not affect the source dictionary
        filters_copy = copy.deepcopy(filters)

        # tas_codes are unique in that the same query is spread across two keys
        must_queries = self._handle_tas_query(must_queries, filters_copy)
        for filter_type, filter_values in filters_copy.items():
            # Validate the filters
            if filter_type in self.unsupported_filters:
                msg = "API request included '{}' key. No filtering will occur with provided value '{}'"
                logger.warning(msg.format(filter_type, filter_values))
                continue
            # elif filter_type not in self.strategy_lookup.keys() and filter_type not in self.nested_filter_lookup.keys():
            #     raise InvalidParameterException(f"Invalid filter: {filter_type} does not exist.")

            # Generate the query for a filter
            if "nested_" in filter_type:
                # Add the "nested_path" option back in if using a nested filter;
                # want to avoid having this option passed to all filters
                nested_options = {**options, "nested_path": nested_path}
                query = self.nested_filter_lookup[filter_type].generate_query(
                    filter_values, self.query_type, **nested_options
                )
                list_pointer = nested_must_queries
            else:
                # strategy = MiscStrategy
                # for strategy_type in self.strategy_map:
                #     if strategy_type in filter_type:
                #         strategy = self.strategy_map[strategy_type]
                #         break
                #
                # # method = getattr(strategy, "get_query")
                # query = getattr(strategy, "get_query")(self, filter_type=filter_type, filter_values=filter_values, query_type=self.query_type)

                query = QueryWithFiltersJobs(AwardsStrategy())
                if self.query_type == QueryType.TRANSACTIONS:
                    query.strategy = TransactionStrategy()
                elif self.query_type == QueryType.SUBAWARDS:
                    query.strategy = SubawardStrategy()
                query.start(
                    filter_name=filter_type,
                    filter_values=filter_values,
                    filter_options=[f"--"]

                list_pointer = must_queries

            # Handle the possibility of multiple queries from one filter
            if isinstance(query, list):
                list_pointer.extend(query)
            else:
                list_pointer.append(query)
        nested_query = ES_Q("nested", path="financial_accounts_by_award", query=ES_Q("bool", must=nested_must_queries))
        if must_queries and nested_must_queries:
            must_queries.append(nested_query)
        elif nested_must_queries:
            must_queries = nested_query
        return ES_Q("bool", must=must_queries)

    def _handle_tas_query(self, must_queries: list, filters: dict) -> list:
        if filters.get(TreasuryAccounts.underscore_name) or filters.get(TasCodes.underscore_name):
            tas_queries = []
            if filters.get(TreasuryAccounts.underscore_name):
                tas_queries.append(
                    TreasuryAccounts.generate_elasticsearch_query(
                        filters[TreasuryAccounts.underscore_name], self.query_type
                    )
                )
            if filters.get(TasCodes.underscore_name):
                tas_queries.append(
                    (TasCodes.generate_elasticsearch_query(filters[TasCodes.underscore_name], self.query_type))
                )
            must_queries.append(ES_Q("bool", should=tas_queries, minimum_should_match=1))
            filters.pop(TreasuryAccounts.underscore_name, None)
            filters.pop(TasCodes.underscore_name, None)
        return must_queries