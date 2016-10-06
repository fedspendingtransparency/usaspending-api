from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer


class TreasuryAppropriationAccountList(APIView):

    """
    List all treasury appropriation accounts
    """
    def get(self, request, format=None):
        taa = TreasuryAppropriationAccount.objects.all()
        serializer = TreasuryAppropriationAccountSerializer(taa, many=True)
        response_object = {
            "count": taa.count(),
            "results": serializer.data
        }
        return Response(response_object)


class AppropriationAccountBalancesList(APIView):

    """
    List all appropriation accounts balances
    """
    def get(self, request, format=None):
        taa = AppropriationAccountBalances.objects.all()
        serializer = AppropriationAccountBalancesSerializer(taa, many=True)
        response_object = {
            "count": taa.count(),
            "results": serializer.data
        }
        return Response(response_object)
