from rest_framework.response import Response
from rest_framework.views import APIView

from collections import OrderedDict
from functools import total_ordering

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.lookups.lookups import award_contracts_mapping, contract_type_mapping, \
    grant_type_mapping, direct_payment_type_mapping, loan_type_mapping, other_type_mapping, \
    loan_award_mapping, non_loan_assistance_award_mapping, non_loan_assistance_type_mapping

import ast
from usaspending_api.common.helpers import generate_fiscal_year, generate_fiscal_period, generate_fiscal_month, \
    get_pagination

import logging
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
        potential_groups = ["quarter", "fiscal_year", "month", "fy", "q", "m"]
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')

        # build sql query filters
        queryset = transaction_filter(filters)
        # define what values are needed in the sql query
        queryset = queryset.values('action_date', 'federal_action_obligation')

        # build response
        response = {'group': group, 'results': []}

        # filter queryset by time
        group_results = OrderedDict()  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000
        queryset = queryset.order_by("action_date").values("action_date", "federal_action_obligation")

        for trans in queryset:
            key = {}
            if group == "fy" or group == "fiscal_year":
                fy = generate_fiscal_year(trans["action_date"])
                key = {"fiscal_year": str(fy)}
            elif group == "m" or group == 'month':
                fy = generate_fiscal_year(trans["action_date"])
                m = generate_fiscal_month(trans["action_date"])
                key = {"fiscal_year": str(fy), "month": str(m)}
            else:  # quarter
                fy = generate_fiscal_year(trans["action_date"])
                q = generate_fiscal_period(trans["action_date"])
                key = {"fiscal_year": str(fy), "quarter": str(q)}

            key = str(key)
            if group_results.get(key) is None:
                group_results[key] = trans["federal_action_obligation"]
            else:
                if trans["federal_action_obligation"]:
                    group_results[key] = group_results.get(key) + trans["federal_action_obligation"]
                else:
                    group_results[key] = group_results.get(key)

        # convert result into expected format
        results = []
        # Expected results structure
        # [{
        # "time_period": {"fy": "2017", "quarter": "3"},
        # 	"aggregated_amount": "200000000"
        # }]
        for key, value in group_results.items():
            key_dict = ast.literal_eval(key)
            result = {"time_period": key_dict, "aggregated_amount": float(value)}
            results.append(result)
        response['results'] = results

        return Response(response)


class SpendingByCategoryVisualizationViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        category = json_request.get('category', None)
        scope = json_request.get('scope', None)
        filters = json_request.get('filters', None)
        limit = json_request.get('limit', 10)
        page = json_request.get('page', 1)

        if category is None:
            raise InvalidParameterException('Missing one or more required request parameters: category')
        potential_categories = ["awarding_agency", "funding_agency", "recipient", "cfda_programs", "industry_codes"]
        if category not in potential_categories:
            raise InvalidParameterException('Category does not have a valid value')
        if (scope is None) and (category != "cfda_programs"):
            raise InvalidParameterException('Missing one or more required request parameters: scope')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')

        # filter queryset
        queryset = transaction_filter(filters)

        # filter the transactions by category
        if category == "awarding_agency":
            potential_scopes = ["agency", "subagency", "offices"]
            if scope not in potential_scopes:
                raise InvalidParameterException('scope does not have a valid value')
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query
            # queryset = queryset.values('federal_action_obligation', 'funding_agency', 'awarding_agency')

            if scope == 'agency':
                for trans in queryset:
                    if (hasattr(trans, "awarding_agency")) and (hasattr(trans.awarding_agency, "toptier_agency")):
                        ttname = trans.awarding_agency.toptier_agency.name
                        if hasattr(name_dict, ttname):
                            name_dict[ttname]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[ttname] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "abbreviation": trans.awarding_agency.toptier_agency.abbreviation}

            elif scope == 'subagency':
                for trans in queryset:
                    if (hasattr(trans, "awarding_agency")) and (hasattr(trans.awarding_agency, "subtier_agency")):
                        if trans.awarding_agency.subtier_agency:
                            stname = trans.awarding_agency.subtier_agency.name
                            if hasattr(name_dict, stname):
                                name_dict[stname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[stname] = {"aggregated_amount": trans.federal_action_obligation,
                                                     "abbreviation": trans.awarding_agency.subtier_agency.abbreviation}
            else:  # offices
                for trans in queryset:
                    if (hasattr(trans, "awarding_agency")) and (hasattr(trans.awarding_agency, "office_agency")):
                        if trans.awarding_agency.office_agency:
                            oname = trans.awarding_agency.office_agency.name
                            if hasattr(name_dict, oname):
                                name_dict[oname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[oname] = {"aggregated_amount": trans.federal_action_obligation,
                                                    "abbreviation": trans.awarding_agency.office_agency.abbreviation}

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

            results = get_pagination(results, limit, page)
            response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
            return Response(response)

        elif category == "funding_agency":
            potential_scopes = ["agency", "subagency", "offices"]
            if scope not in potential_scopes:
                raise InvalidParameterException('scope does not have a valid value')
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query
            # queryset = queryset.values('federal_action_obligation', 'funding_agency', 'awarding_agency')

            if scope == 'agency':
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "toptier_agency")):
                        ttname = trans.funding_agency.toptier_agency.name
                        if hasattr(name_dict, ttname):
                            name_dict[ttname]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[ttname] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "abbreviation": trans.funding_agency.toptier_agency.abbreviation}

            elif scope == 'subagency':
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "subtier_agency")):
                        if trans.funding_agency.subtier_agency:
                            stname = trans.funding_agency.subtier_agency.name
                            if hasattr(name_dict, stname):
                                name_dict[stname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[stname] = {"aggregated_amount": trans.federal_action_obligation,
                                                     "abbreviation": trans.funding_agency.subtier_agency.abbreviation}
            else:  # offices
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "office_agency")):
                        if trans.funding_agency.office_agency:
                            oname = trans.funding_agency.office_agency.name
                            if hasattr(name_dict, oname):
                                name_dict[oname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[oname] = {"aggregated_amount": trans.federal_action_obligation,
                                                    "abbreviation": trans.funding_agency.office_agency.abbreviation}

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

            results = get_pagination(results, limit, page)
            response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
            return Response(response)
        elif category == "recipient":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            # queryset = queryset.values('federal_action_obligation', 'recipient')
            if scope == "duns":
                for trans in queryset:
                    if hasattr(trans, 'recipient'):
                        r_name = trans.recipient.recipient_name
                        if hasattr(name_dict, r_name):
                            name_dict[r_name]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[r_name] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "legal_entity_id": trans.recipient.legal_entity_id}
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
                results = get_pagination(results, limit, page)
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
                return Response(response)

            elif scope == "parent_duns":
                for trans in queryset:
                    if hasattr(trans, 'recipient'):
                        r_name = trans.recipient.recipient_name
                        if hasattr(name_dict, r_name):
                            name_dict[r_name]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[r_name] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "parent_recipient_unique_id":
                                                     trans.recipient.parent_recipient_unique_id}
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
                results = get_pagination(results, limit, page)
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
                return Response(response)
            else:  # recipient_type
                raise InvalidParameterException('recipient type is not yet implemented')

        elif category == "cfda_programs":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            # queryset = queryset.values('federal_action_obligation', 'assistance_data__cgac')
            for trans in queryset:
                if (hasattr(trans, 'assistance_data')) and hasattr(trans.assistance_data, 'cfda'):
                    cfda_program_number = trans.assistance_data.cfda.program_number
                    if hasattr(name_dict, cfda_program_number):
                        name_dict[cfda_program_number]["aggregated_amount"] += trans.federal_action_obligation
                    else:
                        name_dict[cfda_program_number] = {"aggregated_amount": trans.federal_action_obligation,
                                                          "program_title":
                                                              trans.assistance_data.cfda.program_title,
                                                          "popular_name":
                                                              trans.assistance_data.cfda.popular_name}

            # build response
            results = []

            for key, value in name_dict.items():
                results.append({"cfda_program_number": key, "program_title": value["program_title"],
                                "popular_name": value["popular_name"],
                                "aggregated_amount": value["aggregated_amount"]})
            results = get_pagination(results, limit, page)
            response = {'category': category, 'limit': limit, 'page': page,
                        'results': results}
            return Response(response)

        elif category == "industry_codes":  # industry_codes
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            # queryset = queryset.values('federal_action_obligation', 'assistance_data__cgac')

            if scope == "psc":
                for trans in queryset:
                    if hasattr(trans, 'contract_data'):
                        psc = trans.contract_data.product_or_service_code
                        if hasattr(name_dict, psc):
                            name_dict[psc] += trans.federal_action_obligation
                        else:
                            name_dict[psc] = trans.federal_action_obligation

                results = []
                for key, value in name_dict.items():
                    results.append({"psc_code": key,
                                    "aggregated_amount": value})
                results = get_pagination(results, limit, page)
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page,
                            'results': results}
                return Response(response)

            elif scope == "naics":
                for trans in queryset:
                    if hasattr(trans, 'contract_data'):
                        naics = trans.contract_data.naics
                        if hasattr(name_dict, naics):
                            name_dict[naics]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[naics] = {"aggregated_amount": trans.federal_action_obligation,
                                                "naics_description":
                                                    trans.contract_data.naics_description}

                results = []
                for key, value in name_dict.items():
                    results.append({"naics_code": key,
                                    "aggregated_amount": value["aggregated_amount"],
                                    "naics_description": value["naics_description"]})
                results = get_pagination(results, limit, page)
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page,
                            'results': results}
                return Response(response)

            else:  # recipient_type
                raise InvalidParameterException('recipient type is not yet implemented')


class SpendingByGeographyVisualizationViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        scope = json_request.get('scope', None)
        filters = json_request.get('filters', None)

        if scope is None:
            raise InvalidParameterException('Missing one or more required request parameters: scope')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_scopes = ["recipient_location", "place_of_performance"]
        if scope not in potential_scopes:
            raise InvalidParameterException('scope does not have a valid value')

        # build sql query filters
        queryset = transaction_filter(filters)

        # define what values are needed in the sql query
        # queryset = queryset.values('action_date', 'federal_action_obligation')

        # build response
        response = {'scope': scope, 'results': []}

        # key is time period (defined by group), value is federal_action_obligation
        name_dict = {}
        if scope == "recipient_location":
            for trans in queryset:
                if (hasattr(trans, 'recipient') and hasattr(trans.recipient, 'location') and
                        hasattr(trans.recipient.location, "state_code")):
                    state_code = trans.recipient.location.state_code
                    if name_dict.get(state_code):
                        name_dict[state_code] += trans.federal_action_obligation
                    else:
                        name_dict[state_code] = trans.federal_action_obligation

        else:  # place of performance
            for trans in queryset:
                if hasattr(trans, 'place_of_performance') and hasattr(trans.place_of_performance, "state_code"):
                    state_code = trans.place_of_performance.state_code
                    if name_dict.get(state_code):
                        name_dict[state_code] += trans.federal_action_obligation
                    else:
                        name_dict[state_code] = trans.federal_action_obligation

        # convert result into expected format
        results = []

        for key, value in name_dict.items():
            result = {"state_code": key, "aggregated_amount": float(value)}
            results.append(result)
        response['results'] = results

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
        fields = json_request.get('fields', None)
        filters = json_request.get('filters', None)
        order = json_request.get('order', "asc")
        limit = json_request.get('limit', 10)
        page = json_request.get('page', 1)

        if fields is None:
            raise InvalidParameterException('Missing one or more required request parameters: fields')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        if "award_type_codes" not in filters:
            raise InvalidParameterException('Missing one or more required request parameters: filters["award_type_codes"]')
        if order not in ["asc", "desc"]:
            raise InvalidParameterException('Invalid value for order: {}'.format(order))

        sort = json_request.get('sort', fields[0])
        if sort not in fields:
            raise InvalidParameterException('Sort value not found in fields: {}'.format(sort))

        # build sql query filters
        queryset = award_filter(filters)

        # define what values are needed in the sql query
        # queryset = queryset.values('action_date', 'federal_action_obligation')

        # build response
        response = {'limit': limit, 'page': page, 'results': []}
        results = []

        for award in queryset:
            row = {}
            if set(filters["award_type_codes"]) < set(contract_type_mapping):
                for field in fields:
                    try:
                        award_prop = award
                        for prop in award_contracts_mapping[field].split("__"):
                            award_prop = getattr(award_prop, prop)
                    except:
                        award_prop = None
                    row[field] = award_prop
            elif set(filters["award_type_codes"]) < set(loan_type_mapping):  # loans
                for field in fields:
                    try:
                        award_prop = award
                        for prop in loan_award_mapping[field].split("__"):
                            award_prop = getattr(award_prop, prop)
                    except:
                        award_prop = None
                    row[field] = award_prop
            elif set(filters["award_type_codes"]) < set(non_loan_assistance_type_mapping):  # assistance data
                for field in fields:
                    try:
                        award_prop = award
                        for prop in non_loan_assistance_award_mapping[field].split("__"):
                            award_prop = getattr(award_prop, prop)
                    except:
                        award_prop = None
                    row[field] = award_prop
            results.append(row)
        sorted_results = sorted(results, key=lambda result: self.Min if result[sort] is None else result[sort],
                                reverse=(order == "desc"))
        response["results"] = get_pagination(sorted_results, limit, page)
        return Response(response)


class SpendingByAwardCountVisualizationViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        filters = json_request.get('filters', None)

        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')

        # build sql query filters
        queryset = award_filter(filters)

        # define what values are needed in the sql query
        # queryset = queryset.values('action_date', 'federal_action_obligation')

        # build response
        response = {'results': {}}
        results = {"contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}

        for award in queryset:
            if hasattr(award, "latest_transaction") and hasattr(award.latest_transaction, "contract_data") and \
                    hasattr(award.latest_transaction, 'type'):
                if award.latest_transaction.type in contract_type_mapping:
                    results["contracts"] += 1
            elif hasattr(award, "latest_transaction") and hasattr(award.latest_transaction, "assistance_data") and \
                    hasattr(award.latest_transaction, 'type'):
                if award.latest_transaction.type in grant_type_mapping:  # Grants
                    results["grants"] += 1
                elif award.latest_transaction.type in direct_payment_type_mapping:  # Direct Payment
                    results["direct_payments"] += 1
                elif award.latest_transaction.type in loan_type_mapping:  # Loans
                    results["loans"] += 1
                elif award.latest_transaction.type in other_type_mapping:  # Other
                    results["other"] += 1

        response["results"] = results
        return Response(response)
