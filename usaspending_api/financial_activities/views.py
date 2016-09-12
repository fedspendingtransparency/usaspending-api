from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.submissions.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.submissions.serializers import FinancialAccountsByProgramActivityObjectClassSerializer


class FinancialAccountsByProgramActivityObjectClassList(APIView):

    """
    List all financial activites
    """
    def get(self, request, format=None):
        subs = FinancialAccountsByProgramActivityObjectClass.objects.all()
        serializer = FinancialAccountsByProgramActivityObjectClassSerializer(subs, many=True)
        return Response(serializer.data)
