import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.tests.autocomplete import check_autocomplete
from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data(db):
    mommy.make(
        LegalEntity,
        recipient_name="Lunar Colonization Society",
        recipient_unique_id="LCS123")
    mommy.make(
        LegalEntity,
        recipient_name="Cerean Mineral Extraction Corp.",
        recipient_unique_id="CMEC")
    mommy.make(
        LegalEntity,
        recipient_name="LOOK NO DUNS.",
        recipient_unique_id=None)


@pytest.mark.django_db
def test_recipient_autocomplete_success(client, recipients_data):
    """Verify error on bad autocomplete request for recipients."""

    # test for exact match
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'LCS123'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['recipient_unique_id'] == 'LCS123'

    # test for similar matches
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'Lunar', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3

    # test closest match is at the top
    assert resp.data['results'][0]['recipient_name'] == 'Lunar Colonization Society'

    # test furthest match is at the end
    assert resp.data['results'][-1]['recipient_name'] == 'Cerean Mineral Extraction Corp.'


@pytest.mark.django_db
def test_recipient_autocomplete_failure(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
