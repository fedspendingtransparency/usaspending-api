import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status
import json

'''
This set of tests exists to test all example requests used in the API documentation
if any of these tests break, but others do not, you should first check that the specified
request is still valid. If the request is no longer valid, the documentation requires an update
alongside of its test

Note that these tests don't test the accuracy of the responses, just that they return a
200 response code. Testing the accuracy of these requests should be done in relevant app
testing suites
'''


@pytest.fixture(scope="session")
def documentation_test_data():
    mommy.make('awards.Award', _quantity=1, _fill_optional=True)


@pytest.mark.parametrize("url, req", [
    ("/api/v1/awards/", {"filters": []}),
    ("/api/v1/awards/", {"filters":  [{"field": "date_signed", "operation": "greater_than_or_equal", "value": "2016-06-01"}]}),
    ("/api/v1/awards/", {"filters": [{"field": "date_signed", "operation": "greater_than_or_equal", "value": "2016-06-01"}, {"field": "date_signed", "operation": "less_than", "value": "2017-06-01"}]}),
    ("/api/v1/awards/", {"filters": [{"combine_method": "OR", "filters": [{"field": "type", "operation": "equals", "value": "A"}, {"field": "type", "operation": "equals", "value": "B"}]}]}),
    ("/api/v1/awards/", {"filters": [{"field": "date_signed", "operation": "greater_than_or_equal", "value": "2016-06-01"}, {"combine_method": "OR", "filters": [{"field": "type", "operation": "equals", "value": "A"}, {"combine_method": "AND", "filters": [{"field": "type", "operation": "equals", "value": "B"}, {"field": "date_signed", "operation": "less_than", "value": "2017-06-01"}]}]}]}),
    ("/api/v1/awards/", {"filters": [{"field": "recipient__recipient_name", "operation": "equals", "value": "GENERAL ELECTRIC COMPANY"}]}),
    ("/api/v1/awards/", {"fields": ["description", "recipient"]}),
    ("/api/v1/awards/", {"exclude": ["type"]}),
    ("/api/v1/awards/", {"verbose": True}),
    ("/api/v1/awards/", {"fields": ["type"], "filters": [{"field": "date_signed", "operation": "greater_than", "value": "2016-06-01"}]}),
    ("/api/v1/awards/", {"order": ["recipient__recipient_name"]}),
    ("/api/v1/awards/", {"order": ["-recipient__recipient_name"]}),
    ("/api/v1/awards/", {"page": 5, "limit": 10}),
    ("/api/v1/awards/total/", {"field": "total_obligation", "group": "date_signed__fy"}),
    ("/api/v1/awards/total/", {"field": "total_obligation", "group": "date_signed", "aggregate": "count", "date_part": "month"}),
])
@pytest.mark.django_db
def test_intro_tutorial_post_requests(client, url, req, documentation_test_data):
    assert client.post(
        url,
        data=json.dumps(req),
        content_type='application/json').status_code == status.HTTP_200_OK


@pytest.mark.parametrize("url", [
    "/api/v1/awards/",
    "/api/v1/transactions/",
    "/api/v1/awards/?awarding_agency=1788",
    "/api/v1/awards/?type=A&piid=LB01",
    "/api/v1/awards/?page=5&limit=10",
])
@pytest.mark.django_db
def test_intro_tutorial_get_requests(client, url, documentation_test_data):
    assert client.get(
        url).status_code == status.HTTP_200_OK


@pytest.mark.parametrize("url, req", [
    ("/api/v1/awards/", {"filters": [{"field": "awarding_agency__toptier_agency__cgac_code", "operation": "equals", "value": "097"}]}),
    ("/api/v1/awards/", {"filters": [{"field": "type", "operation": "in", "value": ["A", "B", "C", "D"]}]}),
    ("/api/v1/awards/", {"filters": [{"field": "latest_transaction__contract_data", "operation": "is_null", "value": False}]}),
    ("/api/v1/awards/", {"filters": [{"field": "place_of_performance__state_code", "operation": "not_equals", "value": "NJ"}]})

])
@pytest.mark.django_db
def test_recipe_post_requests(client, url, req, documentation_test_data):
    assert client.post(
        url,
        data=json.dumps(req),
        content_type='application/json').status_code == status.HTTP_200_OK


@pytest.mark.parametrize("url", [
    "/api/v1/awards/?awarding_agency__toptier_agency__cgac_code=097",
])
@pytest.mark.django_db
def test_recipe_get_requests(client, url, documentation_test_data):
    assert client.get(
        url).status_code == status.HTTP_200_OK
