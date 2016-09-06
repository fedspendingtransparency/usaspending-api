from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.awards.models import Award
from usaspending_api.awards.serializers import AwardSerializer

class AwardList(APIView):
    """
    List all awards
    """
    def get(self, request, format=None):
        awards = Award.objects.all()
        serializer = AwardSerializer(awards, many=True)
        return Response(serializer.data)
