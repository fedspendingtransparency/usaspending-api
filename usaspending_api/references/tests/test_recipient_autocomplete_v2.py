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
    """Verify test on autocomplete request for recipients."""

    # Test on search_text using string
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'Wey'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    result = resp.data['results']
    parents = result['parent_recipient']

    # Verify single parent result
    assert parents[0].count() == 1
    assert parents[0][0]['recipient_name'] == 'Weyland-Yutani Corp'

    # Check for Recipients results
    recipients = result['recipient']
    # Verify None Recipient ID, non-matching name, and Parent excluded from Recipients
    assert recipients[0].count() == 3

    # Test on on search_text using recipient_unique_id
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'LV426'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    result = resp.data['results']
    parents = result['parent_recipient']

    # Verify no parent result
    assert parents[0].count() == 0

    # Verify single matching recipient
    recipients = result['recipient']
    assert recipients[0].count() == 1
    assert recipients[0][0]['recipient_name'] == 'Weyland-Yutani Vessel, USCSS Nostromo LV426'

    # Test on on search_text using parent_recipient_unique_id
    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({'search_text': 'AF19'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    result = resp.data['results']
    parents = result['parent_recipient']

    # Verify single parent result
    assert parents[0].count() == 1
    assert parents[0][0]['recipient_name'] == 'Tyrell Corporation'

    # Verify single matching recipient
    recipients = result['recipient']
    assert recipients[0].count() == 1
    assert recipients[0][0]['recipient_name'] == 'Replicants'


@pytest.mark.django_db
def test_recipient_autocomplete_failure(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        '/api/v2/autocomplete/recipient/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
