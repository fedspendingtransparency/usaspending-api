import ast
import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from django.db.models import Sum, Count
from django.db.models.functions import ExtractMonth

from collections import OrderedDict
from functools import total_ordering

from datetime import date
from fiscalyear import *

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, get_pagination, get_pagination_metadata
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.lookups.lookups import award_contracts_mapping, contract_type_mapping, \
    loan_type_mapping, loan_award_mapping, non_loan_assistance_award_mapping, non_loan_assistance_type_mapping
from usaspending_api.references.models import Cfda


logger = logging.getLogger(__name__)


class SpendingOverTimeVisualizationViewSet(APIView):

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
            # TODO: Comment back in for office_agency changes
            queryset = queryset.values("awarding_agency__toptier_agency__name",
                                       "awarding_agency__subtier_agency__name",
                                       # "awarding_agency__office_agency__name",
                                       "awarding_agency__toptier_agency__abbreviation",
                                       "awarding_agency__subtier_agency__abbreviation",
                                       # "awarding_agency__office_agency__abbreviation",
                                       "federal_action_obligation")

            total_return_count = 0

            if scope == "agency":
                agency_set = queryset.filter(awarding_agency__toptier_agency__name__isnull=False) \
                        .values('awarding_agency__toptier_agency__name', 'awarding_agency__toptier_agency__abbreviation') \
                        .annotate(federal_action_obligation=Sum('federal_action_obligation'))\
                        .order_by('-federal_action_obligation')

                total_return_count = agency_set.count()

                for trans in agency_set[lower_limit:upper_limit]:
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

                total_return_count = subagency_set.count()

                for trans in subagency_set[lower_limit:upper_limit]:
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

                total_return_count = office_set.count()

                for trans in office_set[lower_limit:upper_limit]:
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
            page_metadata = get_pagination_metadata(total_return_count, limit, page)
            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "funding_agency":

            # TODO: Add "offices" below for office_agency changes
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query
            # TODO: Comment back in for office_agency changes
            queryset = queryset.values("funding_agency__toptier_agency__name",
                                       "funding_agency__subtier_agency__name",
                                       # "funding_agency__office_agency__name",
                                       "funding_agency__toptier_agency__abbreviation",
                                       "funding_agency__subtier_agency__abbreviation",
                                       # "funding_agency__office_agency__abbreviation",
                                       "federal_action_obligation")
            if scope == "agency":
                agency_set = queryset.filter(funding_agency__toptier_agency__name__isnull=False) \
                    .values('funding_agency__toptier_agency__name', 'funding_agency__toptier_agency__abbreviation') \
                    .annotate(federal_action_obligation=Sum('federal_action_obligation')) \
                    .order_by('-federal_action_obligation')

                total_return_count = agency_set.count()

                for trans in agency_set[lower_limit:upper_limit]:
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

                total_return_count = subagency_set.count()

                for trans in subagency_set[lower_limit:upper_limit]:
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

                total_return_count = office_set.count()

                for trans in office_set[lower_limit:upper_limit]:
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
            page_metadata = get_pagination_metadata(total_return_count, limit, page)
            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "recipient":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            queryset = queryset.values("federal_action_obligation",
                                       "recipient",
                                       "recipient__recipient_name",
                                       "recipient__legal_entity_id",
                                       "recipient__parent_recipient_unique_id")
            if scope == "duns":
                for trans in queryset:
                    if trans["recipient"]:
                        r_name = trans["recipient__recipient_name"]
                        r_obl = trans["federal_action_obligation"] if trans["federal_action_obligation"] else 0
                        r_lei = trans["recipient__legal_entity_id"]
                        if r_name in name_dict:
                            name_dict[r_name]["aggregated_amount"] += r_obl
                        else:
                            name_dict[r_name] = {"aggregated_amount": r_obl, "legal_entity_id": r_lei}
                # build response
                results = []
                # [{
                # "recipient_name": key,
                # "legal_entity_id": ttabrev,
                # 	"aggregated_amount": "200000000"
                # },...]
                for key, value in name_dict.items():
                    results.append({"recipient_name": key, "legal_entity_id": value["legal_entity_id"],
                                    "aggregated_amount": value["aggregated_amount"]})
                results = sorted(results, key=lambda result: result["aggregated_amount"], reverse=True)
                results, page_metadata = get_pagination(results, limit, page)
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
        values = ["id", "fain", "piid", "uri"]
        if set(filters["award_type_codes"]) <= set(contract_type_mapping):
            for field in fields:
                try:
                    values.append(award_contracts_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
            for field in fields:
                try:
                    values.append(loan_award_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))
        elif set(filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):  # assistance data
            for field in fields:
                try:
                    values.append(non_loan_assistance_award_mapping[field])
                except:
                    raise InvalidParameterException("Invalid field value: {}".format(field))

        # build sql query filters
        queryset = award_filter(filters).values(*values)

        # build response
        response = {"limit": limit, "results": []}
        results = []

        total_return_count = queryset.count()
        page_metadata = get_pagination_metadata(total_return_count, limit, page)

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

        for award in queryset[lower_limit:upper_limit]:
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
        response["page_metadata"] = page_metadata

        return Response(response)


class SpendingByAwardCountVisualizationViewSet(APIView):

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
