from django.test import TransactionTestCase, Client
from django.core.serializers.json import json
import logging
import os
import pytest


class EndpointTests(TransactionTestCase):
    # Add any fixtures in here so the data is loaded into the database
    fixtures = ["endpoint_fixture_db"]

    """
    Checks data/testing_data/endpoint_testing_data.json for endpoints, requests,
    and responses

    You can add an endpoint in there to have it automatically included in testing.
    Ensure you include all /'s in the URL to avoid a status code 301 (i.e. instead of /awards/summary use /awards/summary/)

    The format for entries in this json file are as follows (NB: if you use method "GET" you don't need request object):
    [
        {
            "url": "endpoint_url"
            "name": "Name of the test that is reported if failure"
            "method": "POST",
            "request_object": {},
            "response_object": {},
            "status_code": 200
        },
        . . .
    ]
    """
    @pytest.mark.django_db
    def test_endpoints(self):
        json_data = open(os.path.join(os.path.dirname(__file__), '../../data/testing_data/endpoint_testing_data.json'))
        endpoints = json.load(json_data)
        json_data.close()
        logger = logging.getLogger('console')

        for endpoint in endpoints:
            url = endpoint.get('url', None)
            method = endpoint.get('method', None)
            request_object = endpoint.get('request_object', None)
            response_object = endpoint.get('response_object', None)
            status_code = endpoint.get('status_code', None)
            logger.info("Running endpoint test: \n\t" + method + " " + url + "\n\t" + endpoint.get('name', "Unnamed"))

            response = None
            if method == "POST":
                response = self.client.post(url, content_type='application/json', data=json.dumps(request_object), format='json')
            elif method == "GET":
                response = self.client.get(url, format='json')

            # Check if the status code is correct
            self.assertEqual(response.status_code, status_code)
            # Check if the response object is correct
            # because the native assertDictEqual cares about order (and we don't)
            # we will convert our data to sets
            self.assertEqual(set(response.data), set(response_object))
