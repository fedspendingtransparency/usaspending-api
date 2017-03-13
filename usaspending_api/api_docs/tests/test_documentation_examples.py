import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

'''
This set of tests exists to test all example requests used in the API documentation
if any of these tests break, but others do not, you should first check that the specified
request is still valid. If the request is no longer valid, the documentation requires an update
alongside of its test

Note that these tests don't test the accuracy of the responses, just that they return a
200 response code. Testing the accuracy of these requests should be done in relevant app
testing suites
'''


@pytest.fixture
def documentation_test_data():
    mommy.make('awards.Award', _quantity=10, _fill_optional=True)


@pytest.mark.django_db
def test_intro_tutorial_requests(client, documentation_test_data):
    pass
