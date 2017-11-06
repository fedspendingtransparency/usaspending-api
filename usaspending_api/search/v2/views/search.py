import ast
import logging

from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_extensions.cache.decorators import cache_response

from django.db.models import Sum, Count, Q, F
from django.db.models.functions import ExtractMonth, Cast
from django.db.models import FloatField

from collections import OrderedDict
from functools import total_ordering

from datetime import date
from fiscalyear import *

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, get_pagination, get_pagination_metadata
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.lookups.lookups import award_contracts_mapping, contract_type_mapping, \
    loan_type_mapping, loan_award_mapping, non_loan_assistance_award_mapping, non_loan_assistance_type_mapping
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda, LegalEntity

import timeit
logger = logging.getLogger(__name__)


class SpendingOverTimeVisualizationViewSet(APIView):

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)

        if group is None:
            raise InvalidParameterException('Missing one or more required request parameters: group')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_groups = ['quarter', 'fiscal_year', 'month', 'fy', 'q', 'm']
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')

        # build sql query filters
        queryset = transaction_filter(filters)
        # define what values are needed in the sql query
        queryset = queryset.values('action_date', 'federal_action_obligation')

        # build response
        response = {'group': group, 'results': []}
        nested_order = ''

        group_results = OrderedDict()  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

        if group == 'fy' or group == 'fiscal_year':

            fy_set = queryset.values('fiscal_year')\
                .annotate(federal_action_obligation=Sum('federal_action_obligation'))

            for trans in fy_set:
                key = {'fiscal_year': str(trans['fiscal_year'])}
                key = str(key)
                group_results[key] = trans['federal_action_obligation']

        elif group == 'm' or group == 'month':

            month_set = queryset.annotate(month=ExtractMonth('action_date')) \
                .values('fiscal_year', 'month') \
                .annotate(federal_action_obligation=Sum('federal_action_obligation'))

            for trans in month_set:
                # Convert month to fiscal month
                fiscal_month = generate_fiscal_month(date(year=2017, day=1, month=trans['month']))

                key = {'fiscal_year': str(trans['fiscal_year']), 'month': str(fiscal_month)}
                key = str(key)
                group_results[key] = trans['federal_action_obligation']
            nested_order = 'month'
        else:  # quarterly, take months and add them up

            month_set = queryset.annotate(month=ExtractMonth('action_date')) \
                .values('fiscal_year', 'month') \
                .annotate(federal_action_obligation=Sum('federal_action_obligation'))

            for trans in month_set:
                # Convert month to quarter
                quarter = FiscalDate(2017, trans['month'], 1).quarter

                key = {'fiscal_year': str(trans['fiscal_year']), 'quarter': str(quarter)}
                key = str(key)

                # If key exists {fy : quarter}, aggregate
                if group_results.get(key) is None:
                    group_results[key] = trans['federal_action_obligation']
                else:
                    if trans['federal_action_obligation']:
                        group_results[key] = group_results.get(key) + trans['federal_action_obligation']
                    else:
                        group_results[key] = group_results.get(key)
            nested_order = 'quarter'

        # convert result into expected format, sort by key to meet front-end specs
        results = []
        # Expected results structure
        # [{
        # 'time_period': {'fy': '2017', 'quarter': '3'},
        # 	'aggregated_amount': '200000000'
        # }]
        for key, value in sorted(group_results.items(), key=lambda k: (ast.literal_eval(k[0])['fiscal_year'],
                                                                       int(ast.literal_eval(k[0])[nested_order])) if nested_order else (ast.literal_eval(k[0])['fiscal_year'])):
            key_dict = ast.literal_eval(key)
            result = {'time_period': key_dict, 'aggregated_amount': float(value)}
            results.append(result)
        response['results'] = results

        return Response(response)


class SpendingByCategoryVisualizationViewSet(APIView):

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
        queryset = transaction_filter(filters)

        # filter the transactions by category
        if category == "awarding_agency":
            # TODO: Add "offices" below for office_agency changes
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query

            if scope == "agency":
                agency_set = queryset.values('awarding_agency__toptier_agency__name', 'awarding_agency__toptier_agency__abbreviation') \
                        .annotate(federal_action_obligation=Sum('federal_action_obligation'))\
                        .order_by('-federal_action_obligation')

                agency_set = agency_set[lower_limit:upper_limit + 1]
                has_next = len(agency_set) > limit
                has_previous = page > 1

                for trans in agency_set[:limit]:
                    ttname = trans["awarding_agency__toptier_agency__name"]
                    ttabv = trans["awarding_agency__toptier_agency__abbreviation"]
                    ttob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if ttname in name_dict:
                        name_dict[ttname]["aggregated_amount"] += ttob
                    else:
                        name_dict[ttname] = {"aggregated_amount": ttob, "abbreviation": ttabv}

            elif scope == "subagency":
                subagency_set = queryset.filter(awarding_agency__subtier_agency__name__isnull=False) \
                        .values('awarding_agency__subtier_agency__name', 'awarding_agency__subtier_agency__abbreviation') \
                        .annotate(federal_action_obligation=Sum('federal_action_obligation'))\
                        .order_by('-federal_action_obligation')

                subagency_set = subagency_set[lower_limit:upper_limit + 1]
                has_next = len(subagency_set) > limit
                has_previous = page > 1

                for trans in subagency_set[:limit]:
                    stname = trans["awarding_agency__subtier_agency__name"]
                    stabv = trans["awarding_agency__subtier_agency__abbreviation"]
                    stob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if stname in name_dict:
                        name_dict[stname]["aggregated_amount"] += stob
                    else:
                        name_dict[stname] = {"aggregated_amount": stob, "abbreviation": stabv}

            else:  # offices
                office_set = queryset.filter(awarding_agency__office_agency__name__isnull=False) \
                        .values('awarding_agency__office_agency__name', 'awarding_agency__office_agency__abbreviation') \
                        .annotate(federal_action_obligation=Sum('federal_action_obligation'))\
                        .order_by('-federal_action_obligation')

                office_set = office_set[lower_limit:upper_limit + 1]
                has_next = len(office_set) > limit
                has_previous = page > 1

                for trans in office_set[:limit]:
                    oname = trans["awarding_agency__office_agency__name"]
                    oabv = trans["awarding_agency__office_agency__abbreviation"]
                    oob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if oname in name_dict:
                        name_dict[oname]["aggregated_amount"] += oob
                    else:
                        name_dict[oname] = {"aggregated_amount": oob, "abbreviation": oabv}

            # build response
            results = []
            # [{
            # "agency_name": ttname,
            # "agency_abbreviation": ttabrev,
            # 	"aggregated_amount": "200000000"
            # },...]
            for key, value in name_dict.items():
                results.append({"agency_name": key, "agency_abbreviation": value["abbreviation"],
                                "aggregated_amount": value["aggregated_amount"]})
            results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": {'page': page, 'hasNext': has_next, 'hasPrevious': has_previous}}
            return Response(response)

        elif category == "funding_agency":

            # TODO: Add "offices" below for office_agency changes
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query

            queryset = queryset.filter(funding_agency__isnull=False)

            if scope == "agency":
                agency_set = queryset.values('funding_agency__toptier_agency__name', 'funding_agency__toptier_agency__abbreviation') \
                    .annotate(federal_action_obligation=Sum('federal_action_obligation')) \
                    .order_by('-federal_action_obligation')

                agency_set = agency_set[lower_limit:upper_limit + 1]
                has_next = len(agency_set) > limit
                has_previous = page > 1

                for trans in agency_set[:limit]:
                    ttname = trans["funding_agency__toptier_agency__name"]
                    ttabv = trans["funding_agency__toptier_agency__abbreviation"]
                    ttob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if ttname in name_dict:
                        name_dict[ttname]["aggregated_amount"] += ttob
                    else:
                        name_dict[ttname] = {"aggregated_amount": ttob, "abbreviation": ttabv}

            elif scope == "subagency":
                subagency_set = queryset.filter(funding_agency__subtier_agency__name__isnull=False) \
                    .values('funding_agency__subtier_agency__name', 'funding_agency__subtier_agency__abbreviation') \
                    .annotate(federal_action_obligation=Sum('federal_action_obligation')) \
                    .order_by('-federal_action_obligation')

                subagency_set = subagency_set[lower_limit:upper_limit + 1]
                has_next = len(subagency_set) > limit
                has_previous = page > 1

                for trans in subagency_set[:limit]:
                    stname = trans["funding_agency__subtier_agency__name"]
                    stabv = trans["funding_agency__subtier_agency__abbreviation"]
                    stob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if stname in name_dict:
                        name_dict[stname]["aggregated_amount"] += stob
                    else:
                        name_dict[stname] = {"aggregated_amount": stob, "abbreviation": stabv}

            else:  # offices
                office_set = queryset.filter(funding_agency__office_agency__name=False) \
                    .values('funding_agency__office_agency__name', 'funding_agency__office_agency__abbreviation') \
                    .annotate(federal_action_obligation=Sum('federal_action_obligation')) \
                    .order_by('-federal_action_obligation')

                office_set = office_set[lower_limit:upper_limit + 1]
                has_next = len(office_set) > limit
                has_previous = page > 1

                for trans in office_set[:limit]:
                    oname = trans["funding_agency__office_agency__name"]
                    oabv = trans["funding_agency__office_agency__abbreviation"]
                    oob = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if oname in name_dict:
                        name_dict[oname]["aggregated_amount"] += oob
                    else:
                        name_dict[oname] = {"aggregated_amount": oob, "abbreviation": oabv}

            # build response
            results = []
            # [{
            # "agency_name": ttname,
            # "agency_abbreviation": ttabrev,
            # 	"aggregated_amount": "200000000"
            # },...]
            for key, value in name_dict.items():
                results.append({"agency_name": key, "agency_abbreviation": value["abbreviation"],
                                "aggregated_amount": value["aggregated_amount"]})
            results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": {'page': page, 'hasNext': has_next, 'hasPrevious': has_previous}}
            return Response(response)

        elif category == "recipient":
            # filter the transactions by scope name
            # name_dict example: {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            name_dict = OrderedDict()

            if scope == "duns":
                # define what values are needed in the sql query
                queryset = queryset.filter(federal_action_obligation__isnull=False).values("recipient_id") \
                               .annotate(aggregated_amount=Sum("federal_action_obligation"),
                                         legal_entity_id=F("recipient_id")) \
                               .values("legal_entity_id", "aggregated_amount")\
                               .order_by("-aggregated_amount")[lower_limit:upper_limit + 1]

                results = list(queryset)
                hasNext = len(results) > limit
                hasPrevious = page > 1
                results = results[:limit]

                legal_entity_mappings = LegalEntity.objects.filter(
                    legal_entity_id__in=[result["legal_entity_id"] for result in results]).values("recipient_name",
                                                                                                  "legal_entity_id")
                legal_entity_mappings = {result["legal_entity_id"]: result["recipient_name"]
                                         for result in legal_entity_mappings}
                for result in results:
                    result["legal_entity_name"] = legal_entity_mappings[result["legal_entity_id"]]

                page_metadata = {"hasNext": hasNext, "hasPrevious": hasPrevious}
                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            elif scope == "parent_duns":
                for trans in queryset:
                    if trans["recipient"]:
                        r_name = trans["recipient__recipient_name"]
                        r_obl = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                        r_prui = trans["recipient__parent_recipient_unique_id"]
                        if r_name in name_dict:
                            name_dict[r_name]["aggregated_amount"] += r_obl
                        else:
                            name_dict[r_name] = {"aggregated_amount": r_obl, "parent_recipient_unique_id": r_prui}
                # build response
                results = []
                # [{
                # "recipient_name": key,
                # "legal_entity_id": ttabrev,
                # 	"aggregated_amount": "200000000"
                # },...]
                for key, value in name_dict.items():
                    results.append({"recipient_name": key, "parent_recipient_unique_id": value["parent_recipient_unique_id"],
                                    "aggregated_amount": value["aggregated_amount"]})
                results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
                results, page_metadata = get_pagination(results, limit, page)
                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)
            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")

        elif category == "cfda_programs":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            queryset = queryset.values("federal_action_obligation",
                                       "assistance_data__cfda_program_title",
                                       "assistance_data__cfda_program_number")

            for trans in queryset:
                if trans["assistance_data__cfda_program_number"]:
                    cfda_program_number = trans["assistance_data__cfda_program_number"]
                    cfda_program_title = trans["assistance_data__cfda_program_title"]
                    cfda_program_name = Cfda.objects.filter(program_title=cfda_program_title,
                                                            program_number=cfda_program_number).first().popular_name
                    cfda_obl = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                    if cfda_program_number in name_dict:
                        name_dict[cfda_program_number]["aggregated_amount"] += cfda_obl
                    else:
                        name_dict[cfda_program_number] = {"aggregated_amount": cfda_obl,
                                                          "program_title": cfda_program_title,
                                                          "popular_name": cfda_program_name}

            # build response
            results = []

            for key, value in name_dict.items():
                results.append({"cfda_program_number": key, "program_title": value["program_title"],
                                "popular_name": value["popular_name"],
                                "aggregated_amount": value["aggregated_amount"]})
            results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
            results, page_metadata = get_pagination(results, limit, page)
            response = {"category": category, "limit": limit, "results": results, "page_metadata": page_metadata}
            return Response(response)

        elif category == "industry_codes":  # industry_codes
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            queryset = queryset.values("federal_action_obligation", "contract_data__product_or_service_code",
                                       "contract_data__naics", "contract_data__naics_description")

            if scope == "psc":
                for trans in queryset:
                    if trans["contract_data__product_or_service_code"]:
                        psc = trans["contract_data__product_or_service_code"]
                        psc_obl = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                        if psc in name_dict:
                            name_dict[psc] += psc_obl
                        else:
                            name_dict[psc] = psc_obl

                results = []
                for key, value in name_dict.items():
                    results.append({"psc_code": key,
                                    "aggregated_amount": value})
                results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
                results, page_metadata = get_pagination(results, limit, page)
                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            elif scope == "naics":
                for trans in queryset:
                    if trans["contract_data__naics"]:
                        naics = trans["contract_data__naics"]
                        naics_obl = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                        naics_desc = trans["contract_data__naics_description"]
                        if naics in name_dict:
                            name_dict[naics]["aggregated_amount"] += naics_obl
                        else:
                            name_dict[naics] = {"aggregated_amount": naics_obl,
                                                "naics_description": naics_desc}

                results = []
                for key, value in name_dict.items():
                    results.append({"naics_code": key,
                                    "aggregated_amount": value["aggregated_amount"],
                                    "naics_description": value["naics_description"]})
                results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
                results, page_metadata = get_pagination(results, limit, page)
                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")


class SpendingByGeographyVisualizationViewSet(APIView):
    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        scope = json_request.get("scope")
        filters = json_request.get("filters", {})
        self.geo_layer = json_request.get("geo_layer")
        self.geo_layer_filters = json_request.get("geo_layer_filters")
        fields_list = []  # fields to include in the aggregate query

        loc_dict = {
            'state': 'state_code',
            'county': 'county_code',
            'district': 'congressional_code'
        }

        model_dict = {
            'place_of_performance': 'place_of_performance',
            'recipient_location': 'recipient__location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get(scope)
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = '{}__{}'.format(scope_field_name, loc_field_name)

        if scope_field_name is None:
            raise InvalidParameterException("Invalid request parameters: scope")

        if loc_field_name is None:
            raise InvalidParameterException("Invalid request parameters: geo_layer")

        # build sql query filters
        self.queryset = transaction_filter(filters)

        if self.geo_layer == 'state':
            # State will have one field (state_code) containing letter A-Z
            kwargs = {'{}__{}'.format(loc_lookup, 'isnull'): False,
                      '{}__{}'.format(loc_lookup, 'iregex'): r'^[A-Z]{2}$'}

            # Only state scope will add its own state code
            # State codes are consistent in db ie AL, AK
            fields_list.append(loc_lookup)

            state_response = {
                'scope': scope,
                'geo_layer': self.geo_layer,
                'results': self.state_results(kwargs, fields_list, loc_lookup)
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = '{}__{}'.format(scope_field_name, loc_dict['state'])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since
            # can't be surfaced by map
            kwargs = {'{}__{}'.format(loc_lookup, 'isnull'): False,
                      '{}__{}'.format(state_lookup, 'isnull'): False,
                      '{}__{}'.format(state_lookup, 'iregex'): r'^[A-Z]{2}$',
                      '{}__{}'.format(loc_lookup, 'iregex'): r'^[0-9]*(\.\d+)?$'}

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name = '{}__{}'.format(scope_field_name, 'county_name')
                fields_list.append(county_name)
                self.county_district_queryset(kwargs, fields_list, loc_lookup, scope_field_name)

                county_response = {
                    'scope': scope,
                    'geo_layer': self.geo_layer,
                    'results': self.county_results(state_lookup, county_name)
                }

                return Response(county_response)
            else:
                self.geo_queryset = self.county_district_queryset(kwargs, fields_list, loc_lookup, scope_field_name)

                district_response = {
                    'scope': scope,
                    'geo_layer': self.geo_layer,
                    'results': self.district_results(state_lookup)
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields, loc_lookup):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{'{}__{}'.format(loc_lookup, 'in'): self.geo_layer_filters})

        self.geo_queryset = self.queryset.filter(**filter_args) \
            .values(*lookup_fields) \
            .annotate(federal_action_obligation=Sum('federal_action_obligation'))

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [
            {
                'shape_code': x[loc_lookup],
                'aggregated_amount': x['federal_action_obligation'],
                'display_name': code_to_state.get(x[loc_lookup])['name'].title()
            } for x in self.geo_queryset.iterator()
        ]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            self.queryset &= geocode_filter_locations(scope_field_name, [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ], 'TransactionNormalized')

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list) \
            .annotate(federal_action_obligation=Sum('federal_action_obligation'),
                      code_as_float=Cast(loc_lookup, FloatField()))

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [
                {
                    'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                    pad_codes(self.geo_layer, x['code_as_float']),
                    'aggregated_amount': x['federal_action_obligation'],
                    'display_name': x[county_name].title()
                }
                for x in self.geo_queryset.iterator()
            ]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [
                {
                    'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                    pad_codes(self.geo_layer, x['code_as_float']),
                    'aggregated_amount': x['federal_action_obligation'],
                    'display_name': x[state_lookup] + '-' + pad_codes(self.geo_layer, x['code_as_float'])
                } for x in self.geo_queryset.iterator()
            ]

        return results


class SpendingByAwardVisualizationViewSet(APIView):

    @total_ordering
    class MinType(object):
        def __le__(self, other):
            return True

        def __eq__(self, other):
            return (self is other)
    Min = MinType()

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        fields = json_request.get("fields", None)
        filters = json_request.get("filters", None)
        order = json_request.get("order", "asc")
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        if fields is None:
            raise InvalidParameterException("Missing one or more required request parameters: fields")
        elif fields == []:
            raise InvalidParameterException("Please provide a field in the fields request parameter.")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")
        if "award_type_codes" not in filters:
            raise InvalidParameterException("Missing one or more required request parameters: filters['award_type_codes']")
        if order not in ["asc", "desc"]:
            raise InvalidParameterException("Invalid value for order: {}".format(order))

        sort = json_request.get("sort", fields[0])
        if sort not in fields:
            raise InvalidParameterException("Sort value not found in fields: {}".format(sort))

        # get a list of values to queryset on instead of pinging the database for every field
        values = ["id"]
        if "Award ID" in fields:
            values += ["fain", "piid", "uri"]
        if set(filters["award_type_codes"]) <= set(contract_type_mapping):
            for field in fields:
                if field == "Award ID":
                    continue
                try:
                    values.append(award_contracts_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
            for field in fields:
                if field == "Award ID":
                    continue
                try:
                    values.append(loan_award_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):  # assistance data
            for field in fields:
                if field == "Award ID":
                    continue
                try:
                    values.append(non_loan_assistance_award_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))

        # build sql query filters
        queryset = award_filter(filters).values(*values)

        # build response
        response = {"limit": limit, "results": []}
        results = []

        # Modify queryset to be ordered if we specify "sort" in the request
        if sort:
            sort_filters = []
            if set(filters["award_type_codes"]) <= set(contract_type_mapping):
                sort_filters = [award_contracts_mapping[sort]]
            elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
                sort_filters = [loan_award_mapping[sort]]
            else:  # assistance data
                sort_filters = [non_loan_assistance_award_mapping[sort]]
            if sort == "Award ID":
                sort_filters = ["piid", "fain", "uri"]
            if order == 'desc':
                sort_filters = ['-' + sort_filter for sort_filter in sort_filters]
            if sort_filters:
                queryset = queryset.order_by(*sort_filters)

        limited_queryset = queryset[lower_limit:upper_limit+1]
        has_next = len(limited_queryset) > limit

        for award in limited_queryset[:limit]:
            row = {"internal_id": award["id"]}
            if set(filters["award_type_codes"]) <= set(contract_type_mapping):
                for field in fields:
                    row[field] = award[award_contracts_mapping[field]]
            elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
                for field in fields:
                    row[field] = award[loan_award_mapping[field]]
            elif set(filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):  # assistance data
                for field in fields:
                    row[field] = award[non_loan_assistance_award_mapping[field]]
            if "Award ID" in fields and not row["Award ID"]:
                for id_type in ["piid", "fain", "uri"]:
                    if award[id_type]:
                        row["Award ID"] = award[id_type]
                        break
            results.append(row)

        sorted_results = sorted(results, key=lambda result: self.Min if result[sort] is None else result[sort],
                                reverse=(order == "desc"))

        response["results"] = sorted_results
        response["page_metadata"] = {'page': page, 'hasNext': has_next}

        return Response(response)


class SpendingByAwardCountVisualizationViewSet(APIView):
    
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        filters = json_request.get("filters", None)

        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        # build sql query filters
        queryset = award_filter(filters)

        # define what values are needed in the sql query
        queryset = queryset.values('category')
        queryset = queryset.annotate(category_count=Count('category')).exclude(category__isnull=True).\
            values('category', 'category_count')

        results = {"contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}

        for award in queryset:
            result_key = award['category'].replace(' ', '_')
            result_key += 's' if result_key not in ['other', 'loans'] else ''
            results[result_key] = award['category_count']

        # build response
        return Response({"results": results})
