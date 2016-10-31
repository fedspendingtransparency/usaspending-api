from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.common.api_request_utils import FilterGenerator, ResponsePaginator


class TreasuryAppropriationAccountList(APIView):

    """
    List all treasury appropriation accounts
    """
    def get(self, request, format=None):
        taa = TreasuryAppropriationAccount.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        taa = taa.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(taa, request_parameters=request.GET)

        serializer = TreasuryAppropriationAccountSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": taa.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }

        return Response(response_object)


class AppropriationAccountBalancesList(APIView):

    """
    List all appropriation accounts balances
    """
    def get(self, request, format=None):
        taa = AppropriationAccountBalances.objects.all()

        fg = FilterGenerator()
        filter_arguments = fg.create_from_get(request.GET)

        taa = taa.filter(**filter_arguments)

        paged_data = ResponsePaginator.get_paged_data(taa, request_parameters=request.GET)

        serializer = AppropriationAccountBalancesSerializer(paged_data, many=True)
        response_object = {
            "total_metadata": {
                "count": taa.count(),
            },
            "page_metadata": {
                "page_number": paged_data.number,
                "num_pages": paged_data.paginator.num_pages,
                "count": len(paged_data),
            },
            "results": serializer.data
        }

        serializer = AppropriationAccountBalancesSerializer(taa, many=True)
        response_object = {
            "count": taa.count(),
            "results": serializer.data
        }
        return Response(response_object)
