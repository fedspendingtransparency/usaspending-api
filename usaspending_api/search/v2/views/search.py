import ast
import logging

from collections import OrderedDict
from datetime import date
from fiscalyear import FiscalDate
from functools import total_ordering

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import ExtractMonth, ExtractYear, Cast, Coalesce

from usaspending_api.awards.models import Subaward
from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import (can_use_view, get_view_queryset, spending_by_award_count,
                                                             spending_by_geography, spending_over_time)
from usaspending_api.awards.v2.lookups.lookups import (award_type_mapping, contract_type_mapping, loan_type_mapping,
                                                       non_loan_assistance_type_mapping, grant_type_mapping,
                                                       contract_subaward_mapping, grant_subaward_mapping)
from usaspending_api.awards.v2.lookups.matview_lookups import (award_contracts_mapping, loan_award_mapping,
                                                               non_loan_assistance_award_mapping)
from usaspending_api.common.exceptions import ElasticsearchConnectionException, InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, generate_fiscal_year, get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda
from usaspending_api.search.v2.elasticsearch_helper import (search_transactions, spending_by_transaction_count,
                                                            spending_by_transaction_sum_and_count)

logger = logging.getLogger(__name__)


class SpendingOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    endpoint_doc: /advanced_award_search/spending_over_time.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)
        subawards = json_request.get('subawards', False)

        if group is None:
            raise InvalidParameterException('Missing one or more required request parameters: group')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_groups = ['quarter', 'fiscal_year', 'month', 'fy', 'q', 'm']
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')
        if type(subawards) is not bool:
            raise InvalidParameterException('subawards does not have a valid value')

        # define what values are needed in the sql query
        # we do not use matviews for Subaward filtering, just the Subaward download filters
        queryset = subaward_filter(filters) if subawards else spending_over_time(filters) \
            .values('action_date', 'federal_action_obligation', 'original_loan_subsidy_cost')

        # build response
        response = {'group': group, 'results': []}
        nested_order = ''

        # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000
        group_results = OrderedDict()

        # for Subawards we extract data from action_date, for Awards we use sum_transaction_amount
        if subawards:
            data_set = queryset.values('award_type'). \
                annotate(month=ExtractMonth('action_date'), year=ExtractYear('action_date'),
                         transaction_amount=Sum('amount')). \
                values('month', 'year', 'transaction_amount')
        else:
            data_set = queryset.values('fiscal_year')
            if not (group == 'fy' or group == 'fiscal_year'):
                # quarterly also takes months and aggregates the data
                data_set = queryset.annotate(month=ExtractMonth('action_date')).values('fiscal_year', 'month')

            filter_types = filters['award_type_codes'] if 'award_type_codes' in filters else award_type_mapping
            data_set = sum_transaction_amount(data_set, filter_types=filter_types)

        for record in data_set:
            # create fiscal year data based on the action_date for Subawards
            if subawards:
                record['fiscal_year'] = generate_fiscal_year(date(record['year'], record['month'], 1))

            # generate unique key by fiscal date, depending on group
            key = {'fiscal_year': str(record['fiscal_year'])}
            if group == 'm' or group == 'month':
                # generate the fiscal month
                key['month'] = generate_fiscal_month(date(year=2017, day=1, month=record['month']))
                nested_order = 'month'
            elif group == 'q' or group == 'quarter':
                # generate the fiscal quarter
                key['quarter'] = FiscalDate(2017, record['month'], 1).quarter
                nested_order = 'quarter'
            key = str(key)

            # if key exists, aggregate
            if group_results.get(key) is None:
                group_results[key] = record['transaction_amount']
            else:
                group_results[key] = group_results.get(key) + record['transaction_amount']

        # convert result into expected format, sort by key to meet front-end specs
        results = []
        # Expected results structure
        # [{
        # 'time_period': {'fy': '2017', 'quarter': '3'},
        # 	'aggregated_amount': '200000000'
        # }]
        sorted_group_results = sorted(
            group_results.items(),
            key=lambda k: (
                ast.literal_eval(k[0])['fiscal_year'],
                int(ast.literal_eval(k[0])[nested_order])) if nested_order else (ast.literal_eval(k[0])['fiscal_year']))

        for key, value in sorted_group_results:
            key_dict = ast.literal_eval(key)
            result = {'time_period': key_dict, 'aggregated_amount': float(value) if value else float(0)}
            results.append(result)
        response['results'] = results

        return Response(response)


class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        # TODO: check logic in name_dict[x]["aggregated_amount"] statements

        json_request = request.data
        category = json_request.get("category", None)
        scope = json_request.get("scope", None)
        filters = json_request.get("filters", None)
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        if category is None:
            raise InvalidParameterException("Missing one or more required request parameters: category")
        potential_categories = ["awarding_agency", "funding_agency", "recipient", "cfda_programs", "industry_codes"]
        if category not in potential_categories:
            raise InvalidParameterException("Category does not have a valid value")
        if (scope is None) and (category != "cfda_programs"):
            raise InvalidParameterException("Missing one or more required request parameters: scope")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        # filter queryset
        queryset = matview_search_filter(filters, UniversalTransactionView)

        filter_types = filters['award_type_codes'] if 'award_type_codes' in filters else award_type_mapping

        # filter the transactions by category
        if category == "awarding_agency":
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")

            if scope == "agency":
                queryset = queryset \
                    .filter(awarding_toptier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('awarding_toptier_agency_name'),
                        agency_abbreviation=F('awarding_toptier_agency_abbreviation'))

            elif scope == "subagency":
                queryset = queryset \
                    .filter(
                        awarding_subtier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('awarding_subtier_agency_name'),
                        agency_abbreviation=F('awarding_subtier_agency_abbreviation'))

            elif scope == "office":
                    # NOT IMPLEMENTED IN UI
                    raise NotImplementedError

            queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types)\
                .order_by('-aggregated_amount')
            results = list(queryset[lower_limit:upper_limit + 1])

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "funding_agency":
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")

            if scope == "agency":
                queryset = queryset \
                    .filter(funding_toptier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('funding_toptier_agency_name'),
                        agency_abbreviation=F('funding_toptier_agency_abbreviation'))

            elif scope == "subagency":
                queryset = queryset \
                    .filter(
                        funding_subtier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('funding_subtier_agency_name'),
                        agency_abbreviation=F('funding_subtier_agency_abbreviation'))

            elif scope == "office":
                # NOT IMPLEMENTED IN UI
                raise NotImplementedError

            queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                .order_by('-aggregated_amount')
            results = list(queryset[lower_limit:upper_limit + 1])

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "recipient":
            if scope == "duns":
                queryset = queryset \
                    .values(legal_entity_id=F("recipient_id"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .order_by('-aggregated_amount') \
                    .values("aggregated_amount", "legal_entity_id", "recipient_name") \
                    .order_by("-aggregated_amount")

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            elif scope == "parent_duns":
                queryset = queryset \
                    .filter(parent_recipient_unique_id__isnull=False)
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types,
                                                  calculate_totals=False) \
                    .values(
                        'aggregated_amount',
                        'recipient_name',
                        'parent_recipient_unique_id') \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "cfda_programs":
            if can_use_view(filters, 'SummaryCfdaNumbersView'):
                queryset = get_view_queryset(filters, 'SummaryCfdaNumbersView')
                queryset = queryset \
                    .filter(
                        federal_action_obligation__isnull=False,
                        cfda_number__isnull=False) \
                    .values(cfda_program_number=F("cfda_number"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .values(
                        "aggregated_amount",
                        "cfda_program_number",
                        program_title=F("cfda_title")) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]
                for trans in results:
                    trans['popular_name'] = None
                    # small DB hit every loop here
                    cfda = Cfda.objects \
                        .filter(
                            program_title=trans['program_title'],
                            program_number=trans['cfda_program_number']) \
                        .values('popular_name').first()

                    if cfda:
                        trans['popular_name'] = cfda['popular_name']

            else:
                queryset = queryset \
                    .filter(
                        cfda_number__isnull=False) \
                    .values(cfda_program_number=F("cfda_number"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .values(
                        "aggregated_amount",
                        "cfda_program_number",
                        popular_name=F("cfda_popular_name"),
                        program_title=F("cfda_title")) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            response = {"category": category, "limit": limit, "results": results, "page_metadata": page_metadata}
            return Response(response)

        elif category == "industry_codes":  # industry_codes
            if scope == "psc":
                if can_use_view(filters, 'SummaryPscCodesView'):
                    queryset = get_view_queryset(filters, 'SummaryPscCodesView')
                    queryset = queryset \
                        .filter(product_or_service_code__isnull=False) \
                        .values(psc_code=F("product_or_service_code"))
                else:
                    queryset = queryset \
                        .filter(psc_code__isnull=False) \
                        .values("psc_code")

                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .order_by('-aggregated_amount')
                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            elif scope == "naics":
                if can_use_view(filters, 'SummaryNaicsCodesView'):
                    queryset = get_view_queryset(filters, 'SummaryNaicsCodesView')
                    queryset = queryset \
                        .filter(naics_code__isnull=False) \
                        .values('naics_code')
                    queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                        .order_by('-aggregated_amount') \
                        .values(
                            'naics_code',
                            'aggregated_amount',
                            'naics_description')
                else:
                    queryset = queryset \
                        .filter(naics_code__isnull=False) \
                        .values("naics_code")
                    queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                        .order_by('-aggregated_amount') \
                        .values(
                            'naics_code',
                            'aggregated_amount',
                            'naics_description')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")


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
        json_request = request.data

        self.subawards = json_request.get("subawards", False)
        self.scope = json_request.get("scope")
        self.filters = json_request.get("filters", {})
        self.geo_layer = json_request.get("geo_layer")
        self.geo_layer_filters = json_request.get("geo_layer_filters")

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {
            'state': 'state_code',
            'county': 'county_code',
            'district': 'congressional_code'
        }

        model_dict = {
            'place_of_performance': 'pop',
            'recipient_location': 'recipient_location',
            'subawards_place_of_performance': 'place_of_performance',
            'subawards_recipient_location': 'recipient__location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get('{}{}'.format('subawards_' if self.subawards else '', self.scope))
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = '{}_{}{}'.format(scope_field_name, '_' if self.subawards else '', loc_field_name)

        if scope_field_name is None:
            raise InvalidParameterException("Invalid request parameters: scope")
        if loc_field_name is None:
            raise InvalidParameterException("Invalid request parameters: geo_layer")
        if type(self.subawards) is not bool:
            raise InvalidParameterException('subawards does not have a valid value')

        if self.subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.queryset = subaward_filter(self.filters)
            self.model_name = Subaward
        else:
            self.queryset, self.model_name = spending_by_geography(self.filters)

        if self.geo_layer == 'state':
            # State will have one field (state_code) containing letter A-Z
            kwargs = {
                '{}_{}country_code'.format(scope_field_name, '_location_' if self.subawards else ''): 'USA',
                '{}'.format('amount__isnull' if self.subawards else 'federal_action_obligation__isnull'): False
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
            state_lookup = '{}_{}{}'.format(scope_field_name, '_' if self.subawards else '', loc_dict['state'])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {
                '{}__isnull'.format('amount' if self.subawards else 'federal_action_obligation'): False
            }

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name_lookup = '{}_{}county_name'.format(scope_field_name, '_' if self.subawards else '')
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
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types) if not self.subawards \
            else self.geo_queryset.annotate(transaction_amount=Sum('amount'))

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
            self.queryset &= geocode_filter_locations(scope_field_name, [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ], self.model_name, not self.subawards)
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs['{}__{}'.format(loc_lookup, 'isnull')] = False
            kwargs['{}__{}'.format(state_lookup, 'isnull')] = False
            kwargs['{}_{}country_code'.format(scope_field_name, '_location_' if self.subawards else '')] = 'USA'
            kwargs['{}__{}'.format(loc_lookup, 'iregex')] = r'^[0-9]*(\.\d+)?$'

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list) \
            .annotate(code_as_float=Cast(loc_lookup, FloatField()))
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types) if not self.subawards \
            else self.geo_queryset.annotate(transaction_amount=Sum('amount'))

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
        json_request = request.data
        fields = json_request.get("fields", None)
        filters = json_request.get("filters", None)
        subawards = json_request.get("subawards", False)
        order = json_request.get("order", "asc")
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        # input validation
        if fields is None:
            raise InvalidParameterException("Missing one or more required request parameters: fields")
        elif len(fields) == 0:
            raise InvalidParameterException("Please provide a field in the fields request parameter.")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")
        if "award_type_codes" not in filters:
            raise InvalidParameterException(
                "Missing one or more required request parameters: filters['award_type_codes']")
        if order not in ["asc", "desc"]:
            raise InvalidParameterException("Invalid value for order: {}".format(order))
        if type(subawards) is not bool:
            raise InvalidParameterException('subawards does not have a valid value')

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
                if set(filters["award_type_codes"]) <= set(contract_type_mapping):  # contracts
                    sort_filters = [award_contracts_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
                    sort_filters = [loan_award_mapping[sort]]
                else:  # assistance data
                    sort_filters = [non_loan_assistance_award_mapping[sort]]

            if sort == "Award ID":
                sort_filters = ["award__piid", "award__fain"] if subawards else ["piid", "fain", "uri"]
            if order == "desc":
                sort_filters = ["-" + sort_filter for sort_filter in sort_filters]

            queryset = queryset.order_by(*sort_filters).values(*list(values))

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


class SpendingByAwardCountVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Grants, etc.)
        endpoint_doc: /advanced_award_search/spending_by_award_count.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        filters = json_request.get("filters", None)
        subawards = json_request.get("subawards", False)
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")
        if type(subawards) is not bool:
            raise InvalidParameterException("subawards does not have a valid value")

        if subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            queryset = subaward_filter(filters)
        else:
            queryset, model = spending_by_award_count(filters)

        if subawards:
            queryset = queryset \
                .values('award_type') \
                .annotate(category_count=Sum('amount'))

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
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
        ]
        models.extend(AWARD_FILTER)
        models.extend(PAGINATION)
        for m in models:
            if m['name'] in ('keyword', 'award_type_codes', 'sort'):
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

        models = [{'name': 'keyword', 'key': 'filters|keyword', 'type': 'text', 'text_type': 'search', 'min': 3}]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_sum_and_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error generating the transaction sums and counts')
        return Response({"results": results})


class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction_count.md

    """
    @cache_response()
    def post(self, request):

        models = [{'name': 'keyword', 'key': 'filters|keyword', 'type': 'text', 'text_type': 'search', 'min': 3}]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error during the aggregations')
        return Response({"results": results})
