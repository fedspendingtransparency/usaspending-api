import json

from django.http import HttpResponse
from django.views import View


class StatusView(View):
    def get(self, request, format=None):
        response_object = {"status": "running"}
        return HttpResponse(json.dumps(response_object))
