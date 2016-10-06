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

        serializer = FinancialAccountsByAwardsTransactionObligationsSerializer(awards, many=True)
        return Response(serializer.data)


class AwardListSummary(APIView):

    """
    List all awards (summary level)
    """
    def get(self, request, uri=None, piid=None, fain=None, format=None):
        awards = None
        if uri:
            awards = Award.objects.filter(uri=uri)
        elif piid:
            awards = Award.objects.filter(piid=piid)
        elif fain:
            awards = Award.objects.filter(fain=fain)
        else:
            awards = Award.objects.all()

        serializer = AwardSerializer(awards, many=True)
        return Response(serializer.data)
