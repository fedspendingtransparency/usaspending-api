import ast
import logging

from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_extensions.cache.decorators import cache_response

from django.db.models import Sum, Count, F
from django.db.models.functions import ExtractMonth

from collections import OrderedDict
from functools import total_ordering

from datetime import date
from fiscalyear import FiscalDate

from usaspending_api.awards.v2.filters.MaterializedView import view_filter, can_use_view
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, get_simple_pagination_metadata
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.lookups.lookups import award_contracts_mapping, contract_type_mapping, \
    loan_type_mapping, loan_award_mapping, non_loan_assistance_award_mapping, non_loan_assistance_type_mapping
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
        if can_use_view(filters):
            queryset = view_filter(filters, 'SummaryView')
        else:
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

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        scope = json_request.get("scope", None)
        filters = json_request.get("filters", None)

        if scope is None:
            raise InvalidParameterException("Missing one or more required request parameters: scope")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")
        potential_scopes = ["recipient_location", "place_of_performance"]
        if scope not in potential_scopes:
            raise InvalidParameterException("scope does not have a valid value")

        # build sql query filters
        queryset = transaction_filter(filters)

        # define what values are needed in the sql query
        queryset = queryset.values("federal_action_obligation",
                                   "recipient",
                                   "recipient__location__state_code",
                                   "place_of_performance__state_code")
        # build response
        response = {"scope": scope, "results": []}

        # key is time period (defined by group), value is federal_action_obligation
        name_dict = {}
        if scope == "recipient_location":

            geo_queryset = queryset.filter(recipient__location__state_code__isnull=False) \
                .values('recipient__location__state_code') \
                .annotate(federal_action_obligation=Sum('federal_action_obligation'))

            for trans in geo_queryset:
                state_code = trans["recipient__location__state_code"]
                name_dict[state_code] = trans["federal_action_obligation"]

        else:  # place of performance

            geo_queryset = queryset.filter(place_of_performance__state_code__isnull=False) \
                .values('place_of_performance__state_code') \
                .annotate(federal_action_obligation=Sum('federal_action_obligation'))

            for trans in geo_queryset:
                state_code = trans["place_of_performance__state_code"]
                name_dict[state_code] = trans["federal_action_obligation"]

        # convert result into expected format
        results = []

        for key, value in name_dict.items():
            result = {"state_code": key, "aggregated_amount": float(value)}
            results.append(result)
        response["results"] = results

        return Response(response)


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

        response = None
        if can_use_view(filters):
            response = self.process_with_view(filters)
        else:
            response = self.process_with_tables(filters)

        return response


    def process_with_view(self, filters):
        """Return all budget function/subfunction titles matching the provided search text"""
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        # build sql query filters
        queryset = view_filter(filters=filters,view_name='SummaryAwardView')
        queryset = queryset.values("category").annotate(category_count=Sum('counts'))

        results = self.get_results(queryset)

        # build response
        return Response({"results": results})


    def process_with_tables(self, filters):
        """Return all budget function/subfunction titles matching the provided search text"""
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        # build sql query filters
        queryset = award_filter(filters)

        # define what values are needed in the sql query
        queryset = queryset.values('category')
        queryset = queryset.annotate(category_count=Count('category')).exclude(category__isnull=True). \
            values('category', 'category_count')

        results = self.get_results(queryset)

        # build response
        return Response({"results": results})


    def get_results(self, queryset):
        results = {"contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}

        for award in queryset:
            result_key = award['category'].replace(' ', '_')
            result_key += 's' if result_key not in ['other', 'loans'] else ''
            results[result_key] = award['category_count']

        return results
