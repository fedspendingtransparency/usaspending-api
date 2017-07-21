import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data(db):
    mommy.make(
        LegalEntity,
        recipient_name='Weyland-Yutani Corp',
        parent_recipient_unique_id='2099',
        recipient_unique_id='2099')
    mommy.make(
        LegalEntity,
        recipient_name='Yutani Corp',
        parent_recipient_unique_id='2049',
        recipient_unique_id='2049')
    mommy.make(
        LegalEntity,
        recipient_name='Tyrell Corporation',
        parent_recipient_unique_id='AF19',
        recipient_unique_id='AF19')
    mommy.make(
        LegalEntity,
        recipient_name='Weyland-Yutani Vessel, USCSS Nostromo LV426',
        parent_recipient_unique_id='2099',
        recipient_unique_id='LV426')
    mommy.make(
        LegalEntity,
        recipient_name='Wey-Yu, USCSS Prometheus LV223',
        parent_recipient_unique_id='2099',
        recipient_unique_id='LV223')
    mommy.make(
        LegalEntity,
        recipient_name='Wey-Yu Vessel, USCSS Covenant Origae-6',
        parent_recipient_unique_id='2099',
        recipient_unique_id='Origae-6')
    mommy.make(
        LegalEntity,
        recipient_name='Replicants',
        parent_recipient_unique_id='AF19',
        recipient_unique_id='Nexus-6')
    mommy.make(
        LegalEntity,
        recipient_name='Weyland-Yutani, Xenomorph XX121',
        parent_recipient_unique_id='2099',
        recipient_unique_id=None)


@pytest.mark.django_db
def test_recipient_autocomplete_success(client, recipients_data):
    """Verify error on bad autocomplete request for recipients."""

    # Test for Parent Recipient Filter
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'Corp'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    result = resp.data['results']
    parents = result['parent_recipient']
    assert parents[0].count() == 3
    assert parents[0][0]['recipient_name'] == 'Yutani Corp'
    assert parents[0][1]['recipient_name'] == 'Weyland-Yutani Corp'
    assert parents[0][2]['recipient_name'] == 'Tyrell Corporation'

    # Test for Similar Matches
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'Weyland-Yutani'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    result = resp.data['results']
    parents = result['parent_recipient']
    assert parents[0].count() == 3

    # Test Order of Closest Matched Parents
    assert parents[0][0]['parent_recipient_unique_id'] == '2099'
    assert parents[0][1]['parent_recipient_unique_id'] == '2049'

    # Test Order of Closest Matched Recipients
    recipients = result['recipient']

    # Verify None Recipient ID excluded
    assert recipients[0].count() == 4

    # Verify Top Match
    assert recipients[0][0]['recipient_unique_id'] == 'LV426'

    # Verify Bottom Match
    assert recipients[0][3]['recipient_unique_id'] == 'Nexus-6'


@pytest.mark.django_db
def test_recipient_autocomplete_failure(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
