from django.shortcuts import render
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer


class SubmissionAttributesList(APIView):

    """
    List all submission attributes
    """
    def get(self, request, format=None):
        subs = SubmissionAttributes.objects.all()
        serializer = SubmissionAttributesSerializer(subs, many=True)
        return Response(serializer.data)
