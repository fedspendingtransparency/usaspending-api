from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.transaction import transaction_filter
import ast
from usaspending_api.common.helpers import generate_fiscal_year, generate_fiscal_period, generate_fiscal_month

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

        # key is time period (defined by group), value is federal_action_obligation
        group_results = {}  # '{"fy": "2017", "quarter": "3"}' : 1000

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
            # python cant have a dict as a key

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
        # [{
        # "time_period": {"fy": "2017", "quarter": "3"},
        # 	"aggregated_amount": "200000000"
        # }]
        for key, value in group_results.items():
            key = ast.literal_eval(key)
            result = {"time_period": key, "aggregated_amount": float(value)}
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
        limit = json_request.get('limit', None)
        page = json_request.get('page', None)

        if category is None:
            raise InvalidParameterException('Missing one or more required request parameters: category')
        if (scope is None) and (category != "cfda_programs"):
            raise InvalidParameterException('Missing one or more required request parameters: scope')
        if limit is None:
            limit = 10
        if page is None:
            page = 0
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')


        # filter queryset
        queryset = transaction_filter(filters)

        # filter the transactions by category
        print("count:{}".format(queryset.count()))
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
                        print("awarding: {}".format(trans.awarding_agency.toptier_agency.name))
                        ttname = trans.awarding_agency.toptier_agency.name
                        if hasattr(name_dict, ttname):
                            name_dict[ttname]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[ttname] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "abbreviation": trans.awarding_agency.toptier_agency.abbreviation}

            elif scope == 'subagency':
                for trans in queryset:
                    if (hasattr(trans, "awarding_agency")) and (hasattr(trans.awarding_agency, "subtier_agency")):
                        print("awarding: {}".format(trans.awarding_agency.subtier_agency.name))
                        if trans.awarding_agency.subtier_agency:
                            stname = trans.awarding_agency.subtier_agency.name
                            if hasattr(name_dict, stname):
                                name_dict[stname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[stname] = {"aggregated_amount": trans.federal_action_obligation,
                                                     "abbreviation": trans.awarding_agency.toptier_agency.abbreviation}
            else:  # offices
                for trans in queryset:
                    if (hasattr(trans, "awarding_agency")) and (hasattr(trans.awarding_agency, "office_agency")):
                        if trans.awarding_agency.office_agency:
                            oname = trans.awarding_agency.office_agency.name
                            if hasattr(name_dict, oname):
                                name_dict[oname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[oname] = {"aggregated_amount": trans.federal_action_obligation,
                                                    "abbreviation": trans.awarding_agency.toptier_agency.abbreviation}

            # build response
            results = []
            # [{
            # "agency_name": ttname,
            # "agency_abbreviation": ttabrev,
            # 	"aggregated_amount": "200000000"
            # },...]
            for key, value in name_dict:
                results.append({"agency_name": key, "agency_abbreviation": value["abbreviation"],
                                "aggregated_amount": value["aggregated_amount"]})

            response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
            return Response(response)

        elif category == "funding_agency":
            potential_scopes = ["agency", "subagency", "offices"]
            if scope not in potential_scopes:
                raise InvalidParameterException('scope does not have a valid value')
            # filter the transactions by scope name
            name_dict = {}  # {ttname: {aggregated_amount: 1000, abbreviation: "tt"}
            # define what values are needed in the sql query
            queryset = queryset.values('federal_action_obligation', 'funding_agency', 'awarding_agency')

            if scope == 'agency':
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "toptier_agency")):
                        print("funding: {}".format(trans.funding_agency.toptier_agency.name))
                        ttname = trans.funding_agency.toptier_agency.name
                        if hasattr(name_dict, ttname):
                            name_dict[ttname]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[ttname] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "abbreviation": trans.funding_agency.toptier_agency.abbreviation}

            elif scope == 'subagency':
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "subtier_agency")):
                        print("funding: {}".format(trans.funding_agency.subtier_agency.name))
                        if trans.funding_agency.subtier_agency:
                            stname = trans.funding_agency.subtier_agency.name
                            if hasattr(name_dict, stname):
                                name_dict[stname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[stname] = {"aggregated_amount": trans.federal_action_obligation,
                                                     "abbreviation": trans.funding_agency.toptier_agency.abbreviation}
            else:  # offices
                for trans in queryset:
                    if (hasattr(trans, "funding_agency")) and (hasattr(trans.funding_agency, "office_agency")):
                        if trans.funding_agency.office_agency:
                            oname = trans.funding_agency.office_agency.name
                            if hasattr(name_dict, oname):
                                name_dict[oname]["aggregated_amount"] += trans.federal_action_obligation
                            else:
                                name_dict[oname] = {"aggregated_amount": trans.federal_action_obligation,
                                                    "abbreviation": trans.funding_agency.toptier_agency.abbreviation}

            # build response
            results = []
            # [{
            # "agency_name": ttname,
            # "agency_abbreviation": ttabrev,
            # 	"aggregated_amount": "200000000"
            # },...]
            for key, value in name_dict:
                results.append({"agency_name": key, "agency_abbreviation": value["abbreviation"],
                                "aggregated_amount": value["aggregated_amount"]})

            response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
            return Response(response)
        elif category == "recipient":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            queryset = queryset.values('federal_action_obligation', 'recipient')
            if scope == "duns":
                for trans in queryset:
                    if hasattr(trans, 'recipient'):
                        print("recipient: {}".format(trans.recipient.recipient_name))
                        r_name = trans.recipient.recipient_name
                        if hasattr(name_dict, r_name):
                            name_dict[r_name]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[r_name] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "legal_entity_id": trans.recipient.get('legal_entity_id')}
                # build response
                results = []
                # [{
                # "recipient_name": key,
                # "legal_entity_id": ttabrev,
                # 	"aggregated_amount": "200000000"
                # },...]
                for key, value in name_dict:
                    results.append({"recipient_name": key, "legal_entity_id": value["legal_entity_id"],
                                    "aggregated_amount": value["aggregated_amount"]})
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
                return Response(response)

            elif scope == "parent_duns":
                for trans in queryset:
                    if hasattr(trans, 'recipient'):
                        print("recipient: {}".format(trans.recipient.recipient_name))
                        r_name = trans.recipient.recipient_name
                        if hasattr(name_dict, r_name):
                            name_dict[r_name]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[r_name] = {"aggregated_amount": trans.federal_action_obligation,
                                                 "parent_recipient_unique_id":
                                                     trans.recipient.get('parent_recipient_unique_id')}
                # build response
                results = []
                # [{
                # "recipient_name": key,
                # "legal_entity_id": ttabrev,
                # 	"aggregated_amount": "200000000"
                # },...]
                for key, value in name_dict:
                    results.append({"recipient_name": key, "parent_recipient_unique_id": value["parent_recipient_unique_id"],
                                    "aggregated_amount": value["aggregated_amount"]})
                response = {'category': category, 'scope': scope, 'limit': limit, 'page': page, 'results': results}
                return Response(response)
            else:  # recipient_type
                raise InvalidParameterException('recipient type is not yet implemented')

        elif category == "cfda_programs":
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            #queryset = queryset.values('federal_action_obligation', 'assistance_data__cgac')
            for trans in queryset:
                if (hasattr(trans, 'assistance_data')) and (trans.assistance_data.get('cfda')):
                    print("cfda: {}".format(trans.assistance_data.cfda.program_number))
                    cfda_program_number = trans.assistance_data.cfda.program_number
                    if hasattr(name_dict, cfda_program_number):
                        name_dict[cfda_program_number]["aggregated_amount"] += trans.federal_action_obligation
                    else:
                        name_dict[cfda_program_number] = {"aggregated_amount": trans.federal_action_obligation,
                                                          "program_title":
                                                              trans.assistance_data.cfda.get('program_title'),
                                                          "popular_name":
                                                              trans.assistance_data.cfda.get('popular_name')}

            # build response
            results = []

            for key, value in name_dict:
                results.append({"cfda_program_number": key, "program_title": value["program_title"],
                                "popular_name": value["popular_name"],
                                "aggregated_amount": value["aggregated_amount"]})
            response = {'category': category, 'limit': limit, 'page': page,
                        'results': results}
            return Response(response)

        else:  # industry_codes
            # filter the transactions by scope name
            name_dict = {}  # {recipient_name: {legal_entity_id: "1111", aggregated_amount: "1111"}
            # define what values are needed in the sql query
            #queryset = queryset.values('federal_action_obligation', 'assistance_data__cgac')

            if scope == "psc":
                for trans in queryset:
                    if hasattr(trans, 'contract_data'):
                        print("psc: {}".format(trans.contract_data.product_or_service_code))
                        psc = trans.contract_data.product_or_service_code
                        if hasattr(name_dict, psc):
                            name_dict[psc] += trans.federal_action_obligation
                        else:
                            name_dict[psc] = trans.federal_action_obligation

                        results = []
                        for key, value in name_dict:
                            results.append({"psc_code": key,
                                            "aggregated_amount": value})
                        response = {'category': category, 'scope': scope, 'limit': limit, 'page': page,
                                    'results': results}
                        return Response(response)

            elif scope == "naics":
                for trans in queryset:
                    if hasattr(trans, 'contract_data'):
                        naics = trans.contract_data.naics
                        print("naics: {}".format(trans.contract_data.naics))
                        if hasattr(name_dict, naics):
                            name_dict[naics]["aggregated_amount"] += trans.federal_action_obligation
                        else:
                            name_dict[naics] = {"aggregated_amount": trans.federal_action_obligation,
                                                "naics_description":
                                                    trans.contract_data.get('naics_description')}

                        results = []
                        for key, value in name_dict:
                            results.append({"psc_code": key,
                                            "aggregated_amount": value})
                        response = {'category': category, 'scope': scope, 'limit': limit, 'page': page,
                                    'results': results}
                        return Response(response)

            else:  # recipient_type
                raise InvalidParameterException('recipient type is not yet implemented')
