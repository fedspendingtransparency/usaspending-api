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
from fiscalyear import FiscalDate

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, get_simple_pagination_metadata
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.lookups.lookups import award_contracts_mapping, contract_type_mapping, \
    loan_type_mapping, loan_award_mapping, non_loan_assistance_award_mapping, non_loan_assistance_type_mapping
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda, LegalEntity

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
        sorted_group_results = sorted(
            group_results.items(),
            key=lambda k: (
                ast.literal_eval(k[0])['fiscal_year'],
                int(ast.literal_eval(k[0])[nested_order])) if nested_order else (ast.literal_eval(k[0])['fiscal_year']))

        for key, value in sorted_group_results:
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
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")

            if scope == "agency":
                agency_set = queryset \
                    .filter(
                        awarding_agency__isnull=False,
                        awarding_agency__toptier_agency__name__isnull=False) \
                    .values(
                        agency_name=F('awarding_agency__toptier_agency__name'),
                        agency_abbreviation=F('awarding_agency__toptier_agency__abbreviation')) \
                    .annotate(aggregated_amount=Sum('federal_action_obligation')) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(agency_set[lower_limit:upper_limit + 1])

            elif scope == "subagency":
                subagency_set = queryset \
                    .filter(
                        awarding_agency__isnull=False,
                        awarding_agency__subtier_agency__name__isnull=False) \
                    .values(
                        agency_name=F('awarding_agency__subtier_agency__name'),
                        agency_abbreviation=F('awarding_agency__subtier_agency__abbreviation')) \
                    .annotate(aggregated_amount=Sum('federal_action_obligation'))\
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(subagency_set[lower_limit:upper_limit + 1])

            elif scope == "office":
                # NOT IMPLEMENTED IN UI
                raise NotImplementedError

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
                agency_set = queryset \
                    .filter(
                        funding_agency__isnull=False,
                        funding_agency__toptier_agency__name__isnull=False) \
                    .values(
                        agency_name=F('funding_agency__toptier_agency__name'),
                        agency_abbreviation=F('funding_agency__toptier_agency__abbreviation')) \
                    .annotate(
                        aggregated_amount=Sum('federal_action_obligation')) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(agency_set[lower_limit:upper_limit + 1])

            elif scope == "subagency":
                subagency_set = queryset \
                    .filter(
                        funding_agency__isnull=False,
                        funding_agency__subtier_agency__name__isnull=False) \
                    .values(
                        agency_name=F('funding_agency__subtier_agency__name'),
                        agency_abbreviation=F('funding_agency__subtier_agency__abbreviation')) \
                    .annotate(
                        aggregated_amount=Sum('federal_action_obligation')) \
                    .order_by('-aggregated_amount')

                results = list(subagency_set[lower_limit:upper_limit + 1])

            elif scope == "office":
                # NOT IMPLEMENTED IN UI
                raise NotImplementedError

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "recipient":
            if scope == "duns":
                queryset = queryset \
                    .filter(federal_action_obligation__isnull=False) \
                    .values(legal_entity_id=F("recipient_id")) \
                    .annotate(aggregated_amount=Sum("federal_action_obligation")) \
                    .values("aggregated_amount", "legal_entity_id") \
                    .order_by("-aggregated_amount")

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                # The below code (here to the `elif`) is necessary due to django ORM
                # Overview: sort list by legal-entity ids, then fetch le names by id,
                #   sort into the same order, then add names to result list.
                #   reorder results by aggregated amount
                results = sorted(results, key=lambda result: result["legal_entity_id"])
                # (Small) DB hit here
                le_names = LegalEntity.objects \
                    .filter(legal_entity_id__in=[result["legal_entity_id"] for result in results]) \
                    .order_by('legal_entity_id') \
                    .values_list('recipient_name', flat=True)

                for i in range(len(results)):
                    results[i]['recipient_name'] = le_names[i]

                results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)

            elif scope == "parent_duns":
                queryset = queryset \
                    .filter(recipient__parent_recipient_unique_id__isnull=False) \
                    .annotate(aggregated_amount=Sum('federal_action_obligation')) \
                    .values(
                        'aggregated_amount',
                        recipient_name=F('recipient__recipient_name'),
                        parent_recipient_unique_id=F('recipient__parent_recipient_unique_id')) \
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
            queryset = queryset \
                .filter(
                    assistance_data__cfda_number__isnull=False,
                    federal_action_obligation__isnull=False) \
                .values(cfda_program_number=F("assistance_data__cfda_number")) \
                .annotate(aggregated_amount=Sum('federal_action_obligation')) \
                .values(
                    "aggregated_amount",
                    "cfda_program_number",
                    program_title=F("assistance_data__cfda_title")) \
                .order_by('-aggregated_amount')

            # Begin DB hits here
            results = list(queryset[lower_limit:upper_limit + 1])

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            for trans in results:
                trans['popular_name'] = None
                # small DB hit every loop here
                cfda = Cfda.objects.filter(
                    program_title=trans['program_title'],
                    program_number=trans['cfda_program_number']).values('popular_name').first()
                if cfda:
                    trans['popular_name'] = cfda['popular_name']

            response = {"category": category, "limit": limit, "results": results, "page_metadata": page_metadata}
            return Response(response)

        elif category == "industry_codes":  # industry_codes
            if scope == "psc":
                queryset = queryset \
                    .filter(contract_data__product_or_service_code__isnull=False) \
                    .values(psc_code=F('contract_data__product_or_service_code')) \
                    .annotate(aggregated_amount=Sum('federal_action_obligation')) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            elif scope == "naics":
                queryset = queryset \
                    .filter(contract_data__naics__isnull=False) \
                    .values(naics_code=F('contract_data__naics')) \
                    .annotate(aggregated_amount=Sum('federal_action_obligation')) \
                    .order_by('-aggregated_amount') \
                    .values(
                        'naics_code',
                        'aggregated_amount',
                        naics_description=F('contract_data__naics_description'))

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
            kwargs = {
                      '{}__location_country_code'.format(scope_field_name): 'USA',
                      'federal_action_obligation__isnull': False
                      }

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
            kwargs = {'federal_action_obligation__isnull': False}

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name = '{}__{}'.format(scope_field_name, 'county_name')
                fields_list.append(county_name)
                self.county_district_queryset(kwargs, fields_list,
                                              loc_lookup, state_lookup, scope_field_name
                                              )

                county_response = {
                    'scope': scope,
                    'geo_layer': self.geo_layer,
                    'results': self.county_results(state_lookup, county_name)
                }

                return Response(county_response)
            else:
                self.county_district_queryset(kwargs, fields_list,
                                              loc_lookup, state_lookup, scope_field_name
                                              )

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
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args['{}__{}'.format(loc_lookup, 'isnull')] = False

        self.geo_queryset = self.queryset.filter(**filter_args) \
            .values(*lookup_fields) \
            .annotate(federal_action_obligation=Sum('federal_action_obligation'))

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [
            {
                'shape_code': x[loc_lookup],
                'aggregated_amount': x['federal_action_obligation'],
                'display_name': code_to_state.get(x[loc_lookup], {'name': 'None'}).get('name').title()
            } for x in self.geo_queryset
        ]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, state_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            self.queryset &= geocode_filter_locations(scope_field_name, [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ], 'TransactionNormalized')
        else:
            # Adding null,USA, not number filters for specific partial index
            # when not using geocode_filter
            kwargs['{}__{}'.format(loc_lookup, 'isnull')] = False
            kwargs['{}__{}'.format(state_lookup, 'isnull')] = False
            kwargs['{}__location_country_code'.format(scope_field_name)] = 'USA'
            kwargs['{}__{}'.format(loc_lookup, 'iregex')] = r'^[0-9]*(\.\d+)?$'

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list) \
            .annotate(federal_action_obligation=Sum('federal_action_obligation'),
                      code_as_float=Cast(loc_lookup, FloatField())
                      )

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [
                {
                    'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                    pad_codes(self.geo_layer, x['code_as_float']),
                    'aggregated_amount': x['federal_action_obligation'],
                    'display_name': x[county_name].title() if x[county_name] is not None
                    else x[county_name]
                }
                for x in self.geo_queryset
            ]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [
                {
                    'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                    pad_codes(self.geo_layer, x['code_as_float']),
                    'aggregated_amount': x['federal_action_obligation'],
                    'display_name': x[state_lookup] + '-' +
                    pad_codes(self.geo_layer, x['code_as_float'])
                } for x in self.geo_queryset
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
            raise InvalidParameterException(
                "Missing one or more required request parameters: filters['award_type_codes']")
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
                except Exception:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
            for field in fields:
                if field == "Award ID":
                    continue
                try:
                    values.append(loan_award_mapping[field])
                except Exception:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):  # assistance data
            for field in fields:
                if field == "Award ID":
                    continue
                try:
                    values.append(non_loan_assistance_award_mapping[field])
                except Exception:
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

        limited_queryset = queryset[lower_limit:upper_limit + 1]
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
