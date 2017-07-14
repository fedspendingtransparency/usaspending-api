import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import TreasuryAppropriationAccount


@pytest.fixture
def recipients_data(db):
    mommy.make(
        TreasuryAppropriationAccount,
        budget_function_title="Income Security",
        budget_subfunction_title="Other Income Security")
    mommy.make(
        TreasuryAppropriationAccount,
        budget_function_title="",
        budget_subfunction_title="National Defense")
    mommy.make(
        TreasuryAppropriationAccount,
        budget_function_title="title",
        budget_subfunction_title="subtitle")


@pytest.mark.django_db
def test_recipient_autocomplete_success(client, recipients_data):
    """Verify error on bad autocomplete request for recipients."""

    # test for budget_function exact match
    resp = client.post(
        '/api/v2/autocomplete/budget_function/',
        content_type='application/json',
        data=json.dumps({'search_text': 'title'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']['budget_function_title']) == 1
    assert resp.data['results']['budget_function_title'][0] == 'title'

    # test for budget_subfunction exact match
    resp = client.post(
        '/api/v2/autocomplete/budget_function/',
        content_type='application/json',
        data=json.dumps({'search_text': 'subtitle'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']['budget_subfunction_title']) == 1
    assert resp.data['results']['budget_subfunction_title'][0] == 'subtitle'

    # test for similar matches
    resp = client.post(
        '/api/v2/autocomplete/budget_function/',
        content_type='application/json',
        data=json.dumps({'search_text': 'income', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']['budget_function_title']) == 3
    assert len(resp.data['results']['budget_subfunction_title']) == 3

    # test closest match is at the top
    assert resp.data['results']['budget_function_title'][0] == 'Income Security'
    assert resp.data['results']['budget_subfunction_title'][0] == 'Other Income Security'

    # test furthest match is at the end
    assert resp.data['results']['budget_function_title'][-1] == ''
    assert resp.data['results']['budget_subfunction_title'][-1] == 'National Defense'


@pytest.mark.django_db
def test_recipient_autocomplete_failure(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        '/api/v2/autocomplete/budget_function/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
