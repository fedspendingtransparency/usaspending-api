from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwardsTransactionObligations, Award
from usaspending_api.awards.serializers import FinancialAccountsByAwardsTransactionObligationsSerializer, AwardSerializer


class AwardList(APIView):

    """
    List all awards (financials)
    """
    def get(self, request, format=None):
        awards = FinancialAccountsByAwardsTransactionObligations.objects.all()
        serializer = FinancialAccountsByAwardsTransactionObligationsSerializer(awards, many=True)
        return Response(serializer.data)


class AwardListSummary(APIView):

    """
    List all awards (summary level)
    """
    def get(self, request, format=None):
        awards = Award.objects.all()
        serializer = AwardSerializer(awards, many=True)
        return Response(serializer.data)