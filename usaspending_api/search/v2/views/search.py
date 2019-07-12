import copy

from django.conf import settings
from django.db.models import Sum, Count, F
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_award_count
from usaspending_api.awards.v2.lookups.lookups import (contract_type_mapping, loan_type_mapping,
                                                       non_loan_assistance_type_mapping, grant_type_mapping,
                                                       contract_subaward_mapping, grant_subaward_mapping,
                                                       idv_type_mapping)
from usaspending_api.awards.v2.lookups.matview_lookups import (award_contracts_mapping, loan_award_mapping,
                                                               non_loan_assistance_award_mapping, award_idv_mapping)
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    endpoint_doc: /advanced_award_search/spending_by_award.md
    """

    @cache_response()
    def post(self, request):
        """Return all awards matching the provided filters and limits"""
        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search', 'min': 1},
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m['name'] in ('award_type_codes', 'fields'):
                m['optional'] = False

        json_request = TinyShield(models).block(request.data)
        fields = json_request["fields"]
        filters = json_request.get("filters", {})
        subawards = json_request["subawards"]
        order = json_request["order"]
        limit = json_request["limit"]
        page = json_request["page"]

        if "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response({
                "limit": limit,
                "results": [],
                "page_metadata": {"page": page, "hasNext": False},
            })

        sort = json_request.get("sort", fields[0])
        if sort not in fields:
            raise InvalidParameterException("Sort value '{}' not found in requested fields: {}".format(sort, fields))

        subawards_values = list(contract_subaward_mapping.keys()) + list(grant_subaward_mapping.keys())
        awards_values = list(award_contracts_mapping.keys()) + list(loan_award_mapping.keys()) + \
            list(non_loan_assistance_award_mapping.keys()) + list(award_idv_mapping.keys())

        msg = "Sort value '{}' not found in {{}} mappings: {{}}".format(sort)
        if not subawards and sort not in awards_values:
            raise InvalidParameterException(msg.format("award", awards_values))
        elif subawards and sort not in subawards_values:
            raise InvalidParameterException(msg.format("subaward", subawards_values))

        # build sql query filters
        if subawards:
            queryset = subaward_filter(filters)
            values = {'subaward_number', 'piid', 'fain', 'award_type'}

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
                if award_idv_mapping.get(field):
                    values.add(award_idv_mapping.get(field))

        # Modify queryset to be ordered by requested "sort" in the request or default value(s)
        if sort:
            if subawards:
                if set(filters["award_type_codes"]) <= set(list(contract_type_mapping)
                                                           + list(idv_type_mapping)):  # Subaward contracts
                    sort_filters = [contract_subaward_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(grant_type_mapping):  # Subaward grants
                    sort_filters = [grant_subaward_mapping[sort]]
                else:
                    msg = """Award Type codes limited for Subawards.
                             Only contracts {}, IDVs {}, or grants {} are available"""
                    msg = msg.format(list(contract_type_mapping.keys()),
                                     list(idv_type_mapping.keys()), list(grant_type_mapping.keys()))
                    raise UnprocessableEntityException(msg)
            else:
                if set(filters["award_type_codes"]) <= set(contract_type_mapping):  # contracts
                    sort_filters = [award_contracts_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
                    sort_filters = [loan_award_mapping[sort]]
                elif set(filters["award_type_codes"]) <= set(idv_type_mapping):  # idvs
                    sort_filters = [award_idv_mapping[sort]]
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

        limited_queryset = queryset[(page - 1) * limit:page * limit + 1]  # lower limit : upper limit
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
                elif award['type'] in idv_type_mapping:
                    for field in fields:
                        row[field] = award.get(award_idv_mapping.get(field))
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

        return Response({"limit": limit, "results": results, "page_metadata": {"page": page, "hasNext": has_next}})


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardCountVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Grants, etc.)
        endpoint_doc: /advanced_award_search/spending_by_award_count.md
    """
    @cache_response()
    def post(self, request):
        models = [{'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False}]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)
        filters = json_request.get("filters", None)
        subawards = json_request["subawards"]
        if filters is None:
            raise InvalidParameterException("Missing required request parameters: 'filters'")

        results = {
            "contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0
        } if not subawards else {
            "subcontracts": 0, "subgrants": 0
        }

        if "award_type_codes" in filters and "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response({"results": results})

        if subawards:
            queryset = subaward_filter(filters)
        else:
            queryset, model = spending_by_award_count(filters)

        if subawards:
            queryset = queryset.values('award_type').annotate(category_count=Count('subaward_id'))
        elif model == 'SummaryAwardView':
            queryset = queryset.values('category').annotate(category_count=Sum('counts'))
        else:
            queryset = queryset.values('category').annotate(category_count=Count('category'))

        categories = {
            'contract': 'contracts',
            'idv': 'idvs',
            'grant': 'grants',
            'direct payment': 'direct_payments',
            'loans': 'loans',
            'other': 'other'
        } if not subawards else {'procurement': 'subcontracts', 'grant': 'subgrants'}

        category_name = 'category' if not subawards else 'award_type'

        # DB hit here
        for award in queryset:
            if award[category_name] is None:
                result_key = 'other' if not subawards else 'subcontracts'
            elif award[category_name] not in categories.keys():
                result_key = 'other'
            else:
                result_key = categories[award[category_name]]

            results[result_key] += award['category_count']

        return Response({"results": results})
