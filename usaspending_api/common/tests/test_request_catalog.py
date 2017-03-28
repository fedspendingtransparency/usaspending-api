import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award


@pytest.fixture
def mock_award_data():
    toptier = mommy.make("references.ToptierAgency", toptier_agency_id=11, name="LEXCORP")
    agency = mommy.make("references.Agency", id=10, toptier_agency=toptier)
    mommy.make(
        'awards.Award',
        id=1234,
        piid='zzz',
        fain='abc123',
        type='B',
        awarding_agency=agency,
        total_obligation=1000,
        description='SMALL BUSINESS ADMINISTRATION',
        date_signed=date(2012, 3, 1))
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000,
        description='small business administration',
        date_signed=date(2012, 3, 1))
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000,
        description='SmaLL BusIness AdMinIstration',
        date_signed=date(2012, 3, 1))
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000,
        description='Large BusIness AdMinIstration',
        date_signed=date(2012, 3, 1))
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='small',
        type='C',
        total_obligation=1000,
        description='LARGE BUSINESS ADMINISTRATION',
        date_signed=date(2012, 3, 1))


@pytest.mark.django_db
def test_request_catalog_generation_and_retrieval(client, mock_award_data):
    resp = client.post(
        '/api/v1/awards/',
        content_type='application/json',
        data=json.dumps({"filters": [{
            "field": ["description", "fain"],
            "operation": "search",
            "value": "small"
        }]}))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    assert len(results) == 4

    checksum = resp.data['req']

    # Check that a post query for this request checksum returns the same thing
    resp = client.post(
        '/api/v1/awards/',
        content_type='application/json',
        data=json.dumps({"req": checksum}))

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # Check that a post query returns the same thing
    resp = client.get('/api/v1/awards/?req=' + checksum)

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # check that a no-good checksum returns 500
    resp = client.get('/api/v1/awards/?req=1234')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
