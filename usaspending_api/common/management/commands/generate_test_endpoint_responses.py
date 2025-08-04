from django.core.management.base import BaseCommand
from django.test import Client
import os
from django.core.serializers.json import json, DjangoJSONEncoder
import logging


class Command(BaseCommand):
    help = "Generates responses matching the requests from endpoint tests. \
            Use this when model updates require updating the responses.\
            You will need to manually load the testing fixture after a db flush \
            Redirect output to a file to save it\
            Usage: `python manage.py generate_test_endpoint_responses`"
    logger = logging.getLogger("console")

    def handle(self, *args, **options):
        # Note, you should load the test fixture into your db after a flush
        # It is not performed in this management command for you because
        # we don't want to accidentally delete important data
        json_data = open(
            os.path.join(os.path.dirname(__file__), "../../../data/testing_data/endpoint_testing_data.json")
        )
        endpoints = json.load(json_data)
        json_data.close()

        # We now have our endpoints. For each endpoint, we will perform the request
        # and then update the dictionary. After, we will serialize it to JSON and
        # print it to the screen, suitable for redirection directly back into the
        # endpoint_testing_data.json file
        c = Client()
        for endpoint in endpoints:
            url = endpoint.get("url", None)
            method = endpoint.get("method", None)
            request_object = endpoint.get("request_object", None)
            status_code = endpoint.get("status_code", None)

            response = None
            if method == "POST":
                response = c.post(url, content_type="application/json", data=json.dumps(request_object), format="json")
            elif method == "GET":
                response = c.get(url, format="json")

            if response.status_code is not status_code:
                raise Exception("Status code mismatch!")

            endpoint["response_object"] = response.data

        print(json.dumps(endpoints, indent=4, cls=DjangoJSONEncoder))
