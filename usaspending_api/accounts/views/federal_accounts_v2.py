from datetime import datetime

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.helpers import fy
from usaspending_api.financial_activities.models import \
    FinancialAccountsByProgramActivityObjectClass
import ast
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from rest_framework.response import Response
from rest_framework_extensions.cache.decorators import cache_response
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.awards.models import FinancialAccountsByAwards
from rest_framework.views import APIView
from django.db.models import Sum, F, Q
from django.utils.dateparse import parse_date
from django.db.models import Sum, F
from fiscalyear import FiscalDate
from collections import OrderedDict
from usaspending_api.awards.v2.filters.filter_helpers import date_or_fy_queryset


def federal_account_filter(queryset, filter):
    for key, value in filter.items():
        if key == 'object_class':
            queryset.objects.filters(object_class_id__in=value)
        elif key == 'program_activity':
            queryset.objects.filters(program_activity_id__in=value)
        elif key == 'time_period':
            success, or_queryset = date_or_fy_queryset(value, FinancialAccountsByAwards, "fiscal_year",
                                                       "reporting_period_start")
            if success:
                queryset &= or_queryset
    return queryset


class ObjectClassFederalAccountsViewSet(APIView):
    """Returns financial spending data by object class."""

    @cache_response()
    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        # create response
        response = {'results': {}}

        # get federal account id from url
        fa_id = int(pk)

        # get FA row
        fa = FederalAccount.objects.filter(id=fa_id).first()
        if fa is None:
            return Response(response)

        # get tas related to FA
        tas_ids = TreasuryAppropriationAccount.objects.filter(federal_account=fa) \
            .values_list('treasury_account_identifier', flat=True)

        # get fin based on tas, select oc, make distinct values
        financial_account_queryset = \
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__in=tas_ids) \
            .select_related('object_class').distinct('object_class')

        # Retrieve only unique major class ids and names
        major_classes = set([(obj.object_class.major_object_class, obj.object_class.major_object_class_name)
                             for obj in financial_account_queryset])
        result = [
            {
                'id': maj[0],
                'name': maj[1],
                'minor_object_class':
                    [
                        {'id': obj[0], 'name': obj[1]}
                        for obj in set([(oc.object_class.object_class, oc.object_class.object_class_name)
                                        for oc in financial_account_queryset
                                        if oc.object_class.major_object_class == maj[0]])
                    ]
            }
            for maj in major_classes
        ]
        return Response({'results': result})


class DescriptionFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):
        return None


class FiscalYearSnapshotFederalAccountsViewSet(APIView):
    @cache_response()
    def get(self, request, pk, format=None):

        queryset = AppropriationAccountBalances.final_objects.filter(treasury_account_identifier__federal_account_id=int(
            pk)).filter(submission__reporting_fiscal_year=fy(datetime.today()))
        queryset = queryset.aggregate(
            outlay=Sum('gross_outlay_amount_by_tas_cpe'),
            budget_authority=Sum('budget_authority_available_amount_total_cpe'),
            obligated=Sum('obligations_incurred_total_by_tas_cpe'),
            unobligated=Sum('unobligated_balance_cpe'),
            balance_brought_forward=Sum(
                    F('budget_authority_unobligated_balance_brought_forward_fyb')
                    +
                    F('adjustments_to_unobligated_balance_brought_forward_cpe')
                    ),
            other_budgetary_resources=Sum('other_budgetary_resources_amount_cpe'),
            appropriations=Sum('budget_authority_appropriated_amount_cpe')
            )
        if queryset['outlay'] is not None:
            return Response({'results': queryset})
        else:
            return Response({})


class SpendingOverTimeFederalAccountsViewSet(APIView):
    @cache_response()
    def post(self, request, pk, format=None):
        # create response
        response = {'results': {}}

        # get federal account id from url
        fa_id = int(pk)
        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)

        nested_order = ''
        group_results = OrderedDict()  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

        financial_account_queryset = \
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__federal_account_id=fa_id)

        if group == 'fy' or group == 'fiscal_year':
            filtered_fa = federal_account_filter(financial_account_queryset, filters).values('certified_date').annotate(
                outlay=F('gross_outlay_amount_by_tas_cpe'),
                obligations_incurred_filtered=F('obligations_incurred_total_by_tas_cpe')
            )

            unfiltered_fa = financial_account_queryset.values('certified_date').annotate(
                obligations_incurred_other=F('obligations_incurred_total_by_tas_cpe'),
                unobliged_balance=F('unobligated_balance_cpe')
            )

            for trans in filtered_fa:
                if trans['certified_date'] is None:
                    continue
                fd = trans['certified_date'].split("-")
                fy = FiscalDate(fd[0], fd[1], fd[2]).year

                key = {'fiscal_year': str(fy)}
                key = str(key)
                if group_results[key] is None:
                    group_results[key] = {"outlay": trans['outlay'] if trans['outlay'] else 0,
                                          "obligations_incurred_filtered": trans["obligations_incurred_filtered"] if trans["obligations_incurred_filtered"] else 0,
                                          "obligations_incurred_other": 0,
                                          "unobliged_balance": 0
                                          }
                else:
                    group_results[key] = {"outlay": group_results[key]["outlay"]+trans['outlay'],
                                          "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"]+trans["obligations_incurred_filtered"],
                                          "obligations_incurred_other": group_results[key]["obligations_incurred_other"],
                                          "unobliged_balance": group_results[key]["unobliged_balance"]
                                          }

            for trans in unfiltered_fa:
                if trans['certified_date'] is None:
                    continue
                fd = trans['certified_date'].split("-")
                fy = FiscalDate(fd[0], fd[1], fd[2]).year

                key = {'fiscal_year': str(fy)}
                key = str(key)

                if group_results[key] is None:
                    group_results[key] = {"outlay": 0,
                                          "obligations_incurred_filtered": 0,
                                          "obligations_incurred_other": trans['obligations_incurred_other'] if trans['obligations_incurred_other'] else 0,
                                          "unobliged_balance": trans["unobliged_balance"] if trans["unobliged_balance"] else 0
                                          }
                else:
                    group_results[key] = {"outlay": group_results[key]["outlay"],
                                          "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"],
                                          "obligations_incurred_other": group_results[key]["obligations_incurred_other"]+trans['obligations_incurred_other'],
                                          "unobliged_balance": group_results[key]["unobliged_balance"]+trans["unobliged_balance"]
                                          }

        else:  # quarterly, take months and add them up

            pass

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
            #result = {'time_period': key_dict, 'aggregated_amount': float(value) if value else float(0)}
            result = value['time_period'] = key_dict
            results.append(result)
        response['results'] = results

        return Response(response)


def filter_on(prefix, key, values):
    if not isinstance(values, (list, tuple)):
        values = [values, ]
    return Q(**{'{}__{}__in'.format(prefix, key): values})


def orred_filter_list(prefix, filters):
    '''Produces Q-object for a list of dicts

    Each dict's (key: value) pairs are ANDed together (rows must satisfy all k:v)
    List items are ORred together (satisfying any one is enough)
    '''
    result = Q()
    for filter in filters:
        subresult = Q()
        for (key, values) in filter.items():
            subresult &= filter_on(prefix, key, values)
        result |= subresult
    return result

def orred_date_filter_list(filters):
    '''Produces Q-object for a list of dicts, each of which may include start and/or end date

    Each dict's (key: value) pairs are ANDed together (rows must satisfy all k:v)
    List items are ORred together (satisfying any one is enough)
    '''
    result = Q()
    for filter in filters:
        subresult = Q()
        if 'start_date' in filter:
            start_date = parse_date(filter['start_date'])
            subresult &= Q(reporting_period_start__gte=start_date)
        if 'end_date' in filter:
            end_date = parse_date(filter['end_date'])
            subresult &= Q(reporting_period_end__lte=end_date)
        result |= subresult
    return result


def federal_account_filter2(filters):
    result = Q()
    for (key, values) in filters.items():
        if key == 'object_class':
            result &= orred_filter_list('object_class', values)
        elif key == 'program_activity':
            result &= filter_on('program_activity', 'program_activity_code', values)
        elif key == 'time_period':
            result &= orred_date_filter_list(values)

    return result


class SpendingByCategoryFederalAccountsViewSet(APIView):

    """

    https://gist.github.com/catherinedevlin/aa510ed9020431d2cbb9cc4045fa5835
    """
    @cache_response()
    def post(self, request, pk, format=None):

        # get fin based on tas, select oc, make distinct values
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__treasury_account_identifier=int(pk))

        # `category` from request determines what to sum over
        # `.annotate`... `F()` is much like using SQL column aliases
        if request.data.get('category') == 'program_activity':
            queryset = queryset.annotate(
                id=F('program_activity_id'),
                code=F('program_activity__program_activity_code'),
                name=F('program_activity__program_activity_name'))
        elif request.data.get('category') == 'object_class':
            queryset = queryset.annotate(
                id=F('object_class_id'),
                code=F('object_class__object_class'),
                name=F('object_class__object_class_name'))
        elif request.data.get('category') == 'treasury_account':
            queryset = queryset.annotate(
                id=F('treasury_account_id'),
                code=F('treasury_account__treasury_account_identifier'),
                name=F('treasury_account__tas_rendering_label'))
        else:
            raise InvalidParameterException("category must be one of: program_activity, object_class, treasury_account")

        filters = federal_account_filter2(request.data['filters'])
        queryset = queryset.filter(filters)

        queryset = queryset.values('id', 'code', 'name').annotate(
            Sum('obligations_incurred_by_program_object_class_cpe'))

        result = {
            "results": {q['name']: q['obligations_incurred_by_program_object_class_cpe__sum']
                        for q in queryset}
        }

        return Response(result)


# class SpendingByAwardCountFederalAccountsViewSet(APIView):
#     @cache_response()
#     def post(self, request, pk, format=None):
#         """Return all budget function/subfunction titles matching the provided search text"""
#         fa_id = int(pk)
#         json_request = request.data
#         filters = json_request.get("filters", None)
#
#         if filters is None:
#             response = {'results': {}}
#
#         faba_queryset = FinancialAccountsByAwards.objects.filter(treasury_account__federal_account_id=fa_id)
#         faba_queryset = federal_account_filter(faba_queryset, filters)
#
#         faba_queryset = faba_queryset.value_list('award_id')
#         queryset = UniversalAwardView.objects.filter(award_id__in=faba_queryset)
#
#         # for IDV CONTRACTS category is null. change to contract
#         queryset = queryset \
#             .values('category') \
#             .annotate(category_count=Count(Coalesce('category', Value('contract')))) \
#             .values('category', 'category_count')
#
#         results = {"contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}
#
#         categories = {
#             'contract': 'contracts',
#             'grant': 'grants',
#             'direct payment': 'direct_payments',
#             'loans': 'loans',
#             'other': 'other'
#         }
#
#         # DB hit here
#         for award in queryset:
#             if award['category'] is None:
#                 result_key = 'contracts'
#             elif award['category'] not in categories.keys():
#                 result_key = 'other'
#             else:
#                 result_key = categories[award['category']]
#             results[result_key] += award['category_count']
#
#         # build response
#         return Response({"results": results})


# class SpendingByAwardFederalAccountsViewSet(APIView):
#     @cache_response()
#     def post(self, request, pk, format=None):
#         """Return all budget function/subfunction titles matching the provided search text"""
#         fa_id = int(pk)
#         json_request = request.data
#         fields = json_request.get("fields", None)
#         filters = json_request.get("filters", None)
#         order = json_request.get("order", "asc")
#         limit = json_request.get("limit", 10)
#         page = json_request.get("page", 1)
#
#         lower_limit = (page - 1) * limit
#         upper_limit = page * limit
#
#         if fields is None:
#             raise InvalidParameterException("Missing one or more required request parameters: fields")
#         elif len(fields) == 0:
#             raise InvalidParameterException("Please provide a field in the fields request parameter.")
#         if filters is None:
#             raise InvalidParameterException("Missing one or more required request parameters: filters")
#         if "award_type_codes" not in filters:
#             raise InvalidParameterException(
#                 "Missing one or more required request parameters: filters['award_type_codes']")
#         if order not in ["asc", "desc"]:
#             raise InvalidParameterException("Invalid value for order: {}".format(order))
#         sort = json_request.get("sort", fields[0])
#         if sort not in fields:
#             raise InvalidParameterException("Sort value not found in fields: {}".format(sort))
#
#         faba_queryset = FinancialAccountsByAwards.objects.filter(treasury_account__federal_account_id=fa_id)
#         faba_queryset = federal_account_filter(faba_queryset, filters)
#
#         faba_queryset = faba_queryset.value_list('award_id')
#         queryset = UniversalAwardView.objects.filter(award_id__in=faba_queryset)
#
#         values = {'award_id', 'piid', 'fain', 'uri', 'type'}  # always get at least these columns
#         for field in fields:
#             if award_contracts_mapping.get(field):
#                 values.add(award_contracts_mapping.get(field))
#             if loan_award_mapping.get(field):
#                 values.add(loan_award_mapping.get(field))
#             if non_loan_assistance_award_mapping.get(field):
#                 values.add(non_loan_assistance_award_mapping.get(field))
#
#         # Modify queryset to be ordered if we specify "sort" in the request
#         if sort:
#             if set(filters["award_type_codes"]) <= set(contract_type_mapping):
#                 sort_filters = [award_contracts_mapping[sort]]
#             elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
#                 sort_filters = [loan_award_mapping[sort]]
#             else:  # assistance data
#                 sort_filters = [non_loan_assistance_award_mapping[sort]]
#
#             if sort == "Award ID":
#                 sort_filters = ["piid", "fain", "uri"]
#             if order == 'desc':
#                 sort_filters = ['-' + sort_filter for sort_filter in sort_filters]
#
#             queryset = queryset.order_by(*sort_filters).values(*list(values))
#
#         limited_queryset = queryset[lower_limit:upper_limit + 1]
#         has_next = len(limited_queryset) > limit
#
#         results = []
#         for award in limited_queryset[:limit]:
#             row = {"internal_id": award["award_id"]}
#
#             if award['type'] in contract_type_mapping:
#                 for field in fields:
#                     row[field] = award.get(award_contracts_mapping.get(field))
#             elif award['type'] in loan_type_mapping:  # loans
#                 for field in fields:
#                     row[field] = award.get(loan_award_mapping.get(field))
#             elif award['type'] in non_loan_assistance_type_mapping:  # assistance data
#                 for field in fields:
#                     row[field] = award.get(non_loan_assistance_award_mapping.get(field))
#
#             if "Award ID" in fields:
#                 for id_type in ["piid", "fain", "uri"]:
#                     if award[id_type]:
#                         row["Award ID"] = award[id_type]
#                         break
#             results.append(row)
#
#         # build response
#         response = {
#             'limit': limit,
#             'results': results,
#             'page_metadata': {
#                 'page': page,
#                 'hasNext': has_next
#             }
#         }
#
#         return Response(response)
