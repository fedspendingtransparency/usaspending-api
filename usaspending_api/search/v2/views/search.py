import logging
import copy

from functools import total_ordering

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import Cast, Coalesce
from django.conf import settings

from usaspending_api.awards.models_matviews import UniversalAwardView, SubawardView
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_award_count, spending_by_geography
from usaspending_api.awards.v2.lookups.lookups import (contract_type_mapping, loan_type_mapping,
                                                       non_loan_assistance_type_mapping, grant_type_mapping,
                                                       contract_subaward_mapping, grant_subaward_mapping)
from usaspending_api.awards.v2.lookups.matview_lookups import (award_contracts_mapping, loan_award_mapping,
                                                               non_loan_assistance_award_mapping)
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.exceptions import (ElasticsearchConnectionException, InvalidParameterException,
                                               UnprocessableEntityException)
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.search.v2.elasticsearch_helper import (search_transactions, spending_by_transaction_count,
                                                            spending_by_transaction_sum_and_count)

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByGeographyVisualizationViewSet(APIView):
    """
        This route takes award filters, and returns spending by state code, county code, or congressional district code.
        endpoint_doc: /advanced award search/spending_by_geography.md
    """
    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
        models = [
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False},
            {'name': 'scope', 'key': 'scope', 'type': 'enum', 'optional': False,
             'enum_values': ['place_of_performance', 'recipient_location']},
            {'name': 'geo_layer', 'key': 'geo_layer', 'type': 'enum', 'optional': False,
             'enum_values': ['state', 'county', 'district']},
            {'name': 'geo_layer_filters', 'key': 'geo_layer_filters', 'type': 'array', 'array_type': 'text',
             'text_type': 'search'}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)

        self.subawards = json_request["subawards"]
        self.scope = json_request["scope"]
        self.filters = json_request.get("filters", None)
        self.geo_layer = json_request["geo_layer"]
        self.geo_layer_filters = json_request.get("geo_layer_filters", None)

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {
            'state': 'state_code',
            'county': 'county_code',
            'district': 'congressional_code'
        }

        model_dict = {
            'place_of_performance': 'pop',
            'recipient_location': 'recipient_location',
            # 'subawards_place_of_performance': 'pop',
            # 'subawards_recipient_location': 'recipient_location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get(self.scope)
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = '{}_{}'.format(scope_field_name, loc_field_name)

        if self.subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.queryset = subaward_filter(self.filters)
            self.model_name = SubawardView
        else:
            self.queryset, self.model_name = spending_by_geography(self.filters)

        if self.geo_layer == 'state':
            # State will have one field (state_code) containing letter A-Z
            column_isnull = 'generated_pragmatic_obligation__isnull'
            if self.subawards:
                column_isnull = 'amount__isnull'
            kwargs = {
                '{}_country_code'.format(scope_field_name): 'USA',
                column_isnull: False
            }

            # Only state scope will add its own state code
            # State codes are consistent in database i.e. AL, AK
            fields_list.append(loc_lookup)

            state_response = {
                'scope': self.scope,
                'geo_layer': self.geo_layer,
                'results': self.state_results(kwargs, fields_list, loc_lookup)
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = '{}_{}'.format(scope_field_name, loc_dict['state'])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {
                '{}__isnull'.format('amount' if self.subawards else 'generated_pragmatic_obligation'): False
            }

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name_lookup = '{}_county_name'.format(scope_field_name)
                fields_list.append(county_name_lookup)
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                county_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.county_results(state_lookup, county_name_lookup)
                }

                return Response(county_response)
            else:
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                district_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.district_results(state_lookup)
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields, loc_lookup):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{'{}__{}'.format(loc_lookup, 'in'): self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args['{}__isnull'.format(loc_lookup)] = False

        self.geo_queryset = self.queryset.filter(**filter_args).values(*lookup_fields)

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum('amount'))
        else:
            self.geo_queryset = self.geo_queryset \
                .annotate(transaction_amount=Sum('generated_pragmatic_obligation')) \
                .values('transaction_amount', *lookup_fields)
        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [{
            'shape_code': x[loc_lookup],
            'aggregated_amount': x['transaction_amount'],
            'display_name': code_to_state.get(x[loc_lookup], {'name': 'None'}).get('name').title()
        } for x in self.geo_queryset]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, state_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            geo_layers_list = [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ]
            self.queryset &= geocode_filter_locations(scope_field_name, geo_layers_list, self.model_name, True)
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs['{}__{}'.format(loc_lookup, 'isnull')] = False
            kwargs['{}__{}'.format(state_lookup, 'isnull')] = False
            kwargs['{}_country_code'.format(scope_field_name)] = 'USA'
            kwargs['{}__{}'.format(loc_lookup, 'iregex')] = r'^[0-9]*(\.\d+)?$'

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list) \
            .annotate(code_as_float=Cast(loc_lookup, FloatField()))

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum('amount'))
        else:
            self.geo_queryset = self.geo_queryset \
                .annotate(transaction_amount=Sum('generated_pragmatic_obligation')) \
                .values('transaction_amount', 'code_as_float', *fields_list)

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [{
            'shape_code': code_to_state.get(x[state_lookup])['fips'] + pad_codes(self.geo_layer, x['code_as_float']),
            'aggregated_amount': x['transaction_amount'],
            'display_name': x[county_name].title() if x[county_name] is not None else x[county_name]
        } for x in self.geo_queryset]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [{
            'shape_code': code_to_state.get(x[state_lookup])['fips'] + pad_codes(self.geo_layer, x['code_as_float']),
            'aggregated_amount': x['transaction_amount'],
            'display_name': x[state_lookup] + '-' + pad_codes(self.geo_layer, x['code_as_float'])
        } for x in self.geo_queryset]

        return results


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    endpoint_doc: /advanced_award_search/spending_by_award.md
    """
    @total_ordering
    class MinType(object):
        def __le__(self, other):
            return True

        def __eq__(self, other):
            return self is other
    Min = MinType()

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m['name'] in ('award_type_codes', 'fields'):
                m['optional'] = False

        json_request = TinyShield(models).block(request.data)
        fields = json_request.get("fields", None)
        filters = json_request.get("filters", None)
        subawards = json_request["subawards"]
        order = json_request["order"]
        limit = json_request["limit"]
        page = json_request["page"]

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        sort = json_request.get("sort", fields[0])
        if sort not in fields:
            raise InvalidParameterException("Sort value not found in fields: {}".format(sort))

        subawards_values = list(contract_subaward_mapping.keys()) + list(grant_subaward_mapping.keys())
        awards_values = list(award_contracts_mapping.keys()) + list(loan_award_mapping) + \
            list(non_loan_assistance_award_mapping.keys())
        if (subawards and sort not in subawards_values) or (not subawards and sort not in awards_values):
            raise InvalidParameterException("Sort value not found in award mappings: {}".format(sort))

        # build sql query filters
        if subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            queryset = subaward_filter(filters)

            values = {'subaward_number', 'award__piid', 'award__fain', 'award_type'}
            for field in fields:
                if contract_subaward_mapping.get(field):
                    values.add(contract_subaward_mapping.get(field))
                if grant_subaward_mapping.get(field):
                    values.add(grant_subaward_mapping.get(field))
        else:
            queryset = matview_search_filter(filters, UniversalAwardView).values()

            values = {'award_id', 'piid', 'fain', 'uri', 'type'}
            for field in fields:
                if award_contracts_mapping.get(field):
                    values.add(award_contracts_mapping.get(field))
                if loan_award_mapping.get(field):
                    values.add(loan_award_mapping.get(field))
                if non_loan_assistance_award_mapping.get(field):
                    values.add(non_loan_assistance_award_mapping.get(field))

        # Modify queryset to be ordered if we specify "sort" in the request
        if sort and "no intersection" not in filters["award_type_codes"]:
            if subawards:
                if set(filters["award_type_codes"]) <= set(contract_type_mapping):  # Subaward contracts
                    sort_filters = [contract_subaward_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(grant_type_mapping):  # Subaward grants
                    sort_filters = [grant_subaward_mapping[sort]]
                else:
                    msg = 'Award Type codes limited for Subawards. Only contracts {} or grants {} are available'
                    msg = msg.format(list(contract_type_mapping.keys()), list(grant_type_mapping.keys()))
                    raise UnprocessableEntityException(msg)
            else:
                if set(filters["award_type_codes"]) <= set(contract_type_mapping):  # contracts
                    sort_filters = [award_contracts_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
                    sort_filters = [loan_award_mapping[sort]]
                else:  # assistance data
                    sort_filters = [non_loan_assistance_award_mapping[sort]]

            # Explictly set NULLS LAST in the ordering to encourage the usage of the indexes
            if sort == "Award ID" and subawards:
                if order == "desc":
                    queryset = queryset.order_by(
                        F('award__piid').desc(nulls_last=True),
                        F('award__fain').desc(nulls_last=True)).values(*list(values))
                else:
                    queryset = queryset.order_by(
                        F('award__piid').asc(nulls_last=True),
                        F('award__fain').asc(nulls_last=True)).values(*list(values))
            elif sort == "Award ID":
                if order == "desc":
                    queryset = queryset.order_by(
                        F('piid').desc(nulls_last=True),
                        F('fain').desc(nulls_last=True),
                        F('uri').desc(nulls_last=True)).values(*list(values))
                else:
                    queryset = queryset.order_by(
                        F('piid').asc(nulls_last=True),
                        F('fain').asc(nulls_last=True),
                        F('uri').asc(nulls_last=True)).values(*list(values))
            elif order == "desc":
                queryset = queryset.order_by(F(sort_filters[0]).desc(nulls_last=True)).values(*list(values))
            else:
                queryset = queryset.order_by(F(sort_filters[0]).asc(nulls_last=True)).values(*list(values))

        limited_queryset = queryset[lower_limit:upper_limit + 1]
        has_next = len(limited_queryset) > limit

        results = []
        for award in limited_queryset[:limit]:
            if subawards:
                row = {"internal_id": award["subaward_number"]}

                if award['award_type'] == 'procurement':
                    for field in fields:
                        row[field] = award.get(contract_subaward_mapping[field])
                elif award['award_type'] == 'grant':
                    for field in fields:
                        row[field] = award.get(grant_subaward_mapping[field])
            else:
                row = {"internal_id": award["award_id"]}

                if award['type'] in loan_type_mapping:  # loans
                    for field in fields:
                        row[field] = award.get(loan_award_mapping.get(field))
                elif award['type'] in non_loan_assistance_type_mapping:  # assistance data
                    for field in fields:
                        row[field] = award.get(non_loan_assistance_award_mapping.get(field))
                elif (award['type'] is None and award['piid']) or award['type'] in contract_type_mapping:
                    # IDV + contract
                    for field in fields:
                        row[field] = award.get(award_contracts_mapping.get(field))

                if "Award ID" in fields:
                    for id_type in ["piid", "fain", "uri"]:
                        if award[id_type]:
                            row["Award ID"] = award[id_type]
                            break
            results.append(row)

        # build response
        response = {
            'limit': limit,
            'results': results,
            'page_metadata': {
                'page': page,
                'hasNext': has_next
            }
        }

        return Response(response)


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardCountVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Grants, etc.)
        endpoint_doc: /advanced_award_search/spending_by_award_count.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        models = [
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        '''for m in models:
            if m['name'] in ('award_type_codes', 'fields'):
                m['optional'] = True'''
        json_request = TinyShield(models).block(request.data)
        filters = json_request.get("filters", None)
        subawards = json_request["subawards"]
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        if subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            queryset = subaward_filter(filters)
        else:
            queryset, model = spending_by_award_count(filters)

        if subawards:
            queryset = queryset \
                .values('award_type') \
                .annotate(category_count=Count('subaward_id'))

        elif model == 'SummaryAwardView':
            queryset = queryset \
                .values('category') \
                .annotate(category_count=Sum('counts'))

        else:
            # for IDV CONTRACTS category is null. change to contract
            queryset = queryset \
                .values('category') \
                .annotate(category_count=Count(Coalesce('category', Value('contract')))) \
                .values('category', 'category_count')

        results = {
            "contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0
        } if not subawards else {
            "subcontracts": 0, "subgrants": 0
        }

        categories = {
            'contract': 'contracts',
            'grant': 'grants',
            'direct payment': 'direct_payments',
            'loans': 'loans',
            'other': 'other'
        } if not subawards else {'procurement': 'subcontracts', 'grant': 'subgrants'}

        category_name = 'category' if not subawards else 'award_type'

        # DB hit here
        for award in queryset:
            if award[category_name] is None:
                result_key = 'contracts' if not subawards else 'subcontracts'
            elif award[category_name] not in categories.keys():
                result_key = 'other'
            else:
                result_key = categories[award[category_name]]
            results[result_key] += award['category_count']

        # build response
        return Response({"results": results})

    #  ###############################  #
        # ELASTIC SEARCH ENDPOINTS #
        # ONLY BELOW THIS POINT    #
    #  ###############################  #


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction.md
    """
    @total_ordering
    class MinType(object):
        def __le__(self, other):
            return True

        def __eq__(self, other):
            return self is other
    Min = MinType()

    @cache_response()
    def post(self, request):

        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text',
             'text_type': 'search', 'optional': False},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m['name'] in ('keywords', 'award_type_codes', 'sort'):
                m['optional'] = False
        validated_payload = TinyShield(models).block(request.data)

        if validated_payload['sort'] not in validated_payload['fields']:
            raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload['sort']))

        lower_limit = (validated_payload['page'] - 1) * validated_payload['limit']
        success, response, total = search_transactions(validated_payload, lower_limit, validated_payload['limit'] + 1)
        if not success:
            raise InvalidParameterException(response)

        metadata = get_simple_pagination_metadata(len(response), validated_payload['limit'], validated_payload['page'])

        results = []
        for transaction in response[:validated_payload['limit']]:
            results.append(transaction)

        response = {
            'limit': validated_payload['limit'],
            'results': results,
            'page_metadata': metadata
        }
        return Response(response)


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class TransactionSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of transactions and summation of federal action obligations.
        endpoint_doc: /advanced_award_search/transaction_spending_summary.md
    """
    @cache_response()
    def post(self, request):
        """
            Returns a summary of transactions which match the award search filter
                Desired values:
                    total number of transactions `award_count`
                    The federal_action_obligation sum of all those transactions `award_spending`

            *Note* Only deals with prime awards, future plans to include sub-awards.
        """

        models = [{'name': 'keywords', 'key': 'filters|keywords', 'type': 'array',
                  'array_type': 'text', 'text_type': 'search', 'optional': False, 'text_min': 3}]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_sum_and_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error generating the transaction sums and counts')
        return Response({"results": results})


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction_count.md

    """
    @cache_response()
    def post(self, request):

        models = [{'name': 'keywords', 'key': 'filters|keywords', 'type': 'array', 'array_type': 'text',
                  'text_type': 'search', 'optional': False, 'text_min': 3}]
        validated_payload = TinyShield(models).block(request.data)
        results = spending_by_transaction_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error during the aggregations')
        return Response({"results": results})
