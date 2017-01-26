from django.core.management import call_command
from django.core.serializers.json import json
import logging
import os
import pytest


@pytest.fixture(scope="session")
def endpoint_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')


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
def test_endpoints(endpoint_data, client):
    json_data = open(
        os.path.join(
            os.path.dirname(__file__),
            '../../data/testing_data/endpoint_testing_data.json'))
    endpoints = json.load(json_data)
    json_data.close()
    logger = logging.getLogger('console')

    # TESTING TODO: fix this hand-rolled parameterization?

    for endpoint in endpoints:
        url = endpoint.get('url', None)
        method = endpoint.get('method', None)
        request_object = endpoint.get('request_object', None)
        response_object = endpoint.get('response_object', None)
        status_code = endpoint.get('status_code', None)
        logger.info("Running endpoint test: \n\t" + method + " " + url + "\n\t"
                    + endpoint.get('name', "Unnamed"))

        response = None
        if method == "POST":
            response = client.post(
                url,
                content_type='application/json',
                data=json.dumps(request_object),
                format='json')
        elif method == "GET":
            response = client.get(url, format='json')

        # Check if the status code is correct
        assert response.status_code == status_code
        # Check if the response object is correct
        # We use a special equivalence check because the response object can
        # be a multi-tiered nest of lists and objects, and can also be OrderedDicts
        # and ResultLists, which don't play nice with the native equality checks
        # TESTING TODO: I bet I can beat this
        assert evaluateEquivalence(response_object, response.data)


def evaluateEquivalence(item1, item2):
    logger = logging.getLogger('console')
    equality = True
    if isinstance(item1, list):
        if len(item1) is not len(item2):
            return False  # Length mismatch on lists mean they can't be equal
        else:
            for i in range(len(item1)):
                matched = False
                for item in item2:
                    if evaluateEquivalence(item1[i], item):
                        item2.remove(
                            item
                        )  # Remove this item from the list if we hit a match
                        matched = True
                        break
                if matched:
                    continue
                return False  # No match in the list for this item
    elif isinstance(item1, dict):
        item2 = dict(item2)  # Make sure this isn't an ordered dict
        for key in item1.keys():
            if key not in item2:  # If the other dict doesn't have this key, we don't have a match
                return False
            if 'date' in key:  # Date fields don't play nicely with the database, so skip them
                continue
            equality = equality and evaluateEquivalence(item1[key], item2[key])
    else:
        # Converting to string represenations to clear up some issues with Decimal casting
        equality = equality and (str(item1) == str(item2))
    return equality
