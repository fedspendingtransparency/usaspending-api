import json

from django.http import HttpRequest, HttpResponse
from django.views import View


class StatusView(View):
    def get(self, request: HttpRequest) -> HttpResponse:
        return HttpResponse(json.dumps({"status": "running"}))
