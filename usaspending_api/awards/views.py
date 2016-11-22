from django.shortcuts import render
from django.db.models import Q, Sum
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwardsTransactionObligations, Award
from usaspending_api.awards.serializers import FinancialAccountsByAwardsTransactionObligationsSerializer, AwardSerializer
from usaspending_api.common.api_request_utils import FilterGenerator, FiscalYear, ResponsePaginator, AutoCompleteHandler
import json


class AwardList(APIView):

    """
    List all awards (financials)
    """
    def get(self, request, uri=None, piid=None, fain=None, format=None):
        awards = None
        if uri:
            awards = FinancialAccountsByAwardsTransactionObligations.objects.filter(financial_accounts_by_awards__uri=uri)
        elif piid:
            awards = FinancialAccountsByAwardsTransactionObligations.objects.filter(financial_accounts_by_awards__piid=piid)
        elif fain:
            awards = FinancialAccountsByAwardsTransactionObligations.objects.filter(financial_accounts_by_awards__fain=fain)
        else:
            awards = FinancialAccountsByAwardsTransactionObligations.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        awards = awards.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(awards, request_parameters=request.GET)

        serializer = FinancialAccountsByAwardsTransactionObligationsSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": awards.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }
        return Response(response_object)


# Autocomplete support for award summary objects
class AwardListSummaryAutocomplete(APIView):
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            return Response(AutoCompleteHandler.handle(Award.objects.all(), body))
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AwardListSummary(APIView):
    def post(self, request, format=None):
        fg = FilterGenerator()
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            filters = fg.create_from_post(body)
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        awards = Award.objects.all()

        if len(fg.search_vectors) > 0:
            vector_sum = fg.search_vectors[0]
            for vector in fg.search_vectors[1:]:
                vector_sum += vector
            awards = awards.annotate(search=vector_sum)

        awards = awards.filter(filters)

        paged_data = ResponsePaginator.get_paged_data(awards, request_parameters=body)

        serializer = AwardSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": awards.count(),
                "total_obligation_sum": awards.aggregate(Sum('total_obligation'))["total_obligation__sum"],
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
                "total_obligation_sum": paged_data.object_list.aggregate(Sum('total_obligation'))["total_obligation__sum"],
            },
            "results": serializer.data
        }
        return Response(response_object)

    """
    List all awards (summary level)
    """
    def get(self, request, uri=None, piid=None, fain=None, format=None):
        filter_map = {
            'awarding_fpds': 'awarding_agency__fpds_code',
            'funding_fpds': 'funding_agency__fpds_code',
        }
        fg = FilterGenerator(filter_map=filter_map, ignored_parameters=['fy'])
        filter_arguments = fg.create_from_get(request.GET)
        # We need to parse the FY to be the appropriate value
        if 'fy' in request.GET:
            fy = FiscalYear(request.GET.get('fy'))
            fy_arguments = fy.get_filter_object('date_signed', as_dict=True)
            filter_arguments = {**filter_arguments, **fy_arguments}

        if uri:
            filter_arguments['uri'] = uri
        elif piid:
            filter_arguments['piid'] = piid
        elif fain:
            filter_arguments['fain'] = fain

        awards = Award.objects.all().filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(awards, request_parameters=request.GET)

        serializer = AwardSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": awards.count(),
                "total_obligation_sum": awards.aggregate(Sum('total_obligation'))["total_obligation__sum"],
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
                "total_obligation_sum": paged_data.object_list.aggregate(Sum('total_obligation'))["total_obligation__sum"],
            },
            "results": serializer.data
        }
        return Response(response_object)
