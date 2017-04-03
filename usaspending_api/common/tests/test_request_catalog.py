import pytest
import json
from datetime import date
from collections import namedtuple

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.common.models import RequestCatalog


@pytest.fixture
def mock_request_catalog_data():
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


@pytest.mark.django_db(transaction=True)
def test_request_catalog_generation_and_retrieval(client, mock_request_catalog_data):
    request = {"filters": [{
        "field": "type",
        "operation": "equals",
        "value": "C"
    }]}

    resp = client.post(
        '/api/v1/awards/',
        content_type='application/json',
        data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    assert len(results) == 1

    # Spoof a real request with a named tuple
    reqStruct = namedtuple('reqStruct', 'query_params data')
    created, req = RequestCatalog.get_or_create_from_request(reqStruct(query_params={}, data=request))

    # Check that a post query for this request checksum returns the same thing
    resp = client.post(
        '/api/v1/awards/',
        content_type='application/json',
        data=json.dumps({"req": req.checksum}))

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # Check that a get query returns the same thing
    resp = client.get('/api/v1/awards/?req=' + req.checksum)

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # check that a no-good checksum returns 400
    resp = client.get('/api/v1/awards/?req=1234')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db(transaction=True)
def test_request_catalog_on_aggregate_endpoints(client, mock_request_catalog_data):
    request = {"filters": [{
        "field": "description",
        "operation": "search",
        "value": "small"
    }],
        "field": "total_obligation",
        "group": "type",
        "aggregate": "sum"
    }

    resp = client.post(
        '/api/v1/awards/total/',
        content_type='application/json',
        data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    assert len(results) == 1
    assert results[0]["item"] == "B"
    assert results[0]["aggregate"] == "3000.00"

    # Spoof a real request with a named tuple
    reqStruct = namedtuple('reqStruct', 'query_params data')
    created, req = RequestCatalog.get_or_create_from_request(reqStruct(query_params={}, data=request))

    # Check that a post query for this request checksum returns the same thing
    resp = client.post(
        '/api/v1/awards/total/',
        content_type='application/json',
        data=json.dumps({"req": req.checksum}))

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # Check that a post query returns the same thing
    resp = client.get('/api/v1/awards/total/?req=' + req.checksum)

    assert resp.status_code == status.HTTP_200_OK
    assert results == resp.data['results']

    # check that a no-good checksum returns 500
    resp = client.get('/api/v1/awards/total/?req=1234')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db(transaction=True)
def test_request_catalog_checksum_collision(client, mock_request_catalog_data):
    request = {"filters": [{
        "field": "description",
        "operation": "search",
        "value": "small"
    }],
        "field": "total_obligation",
        "group": "type",
        "aggregate": "sum"
    }

    # Spoof a real request with a named tuple
    reqStruct = namedtuple('reqStruct', 'query_params data')
    created, req = RequestCatalog.get_or_create_from_request(reqStruct(query_params={}, data=request))

    checksum = req.checksum  # Steal the checksum
    req.delete()

    # Create a new request catalog with the same checksum but different request
    req_collide = RequestCatalog.objects.create(checksum=checksum, request=reqStruct(query_params={}, data={}))

    # Create the same request catalog again
    created, req = RequestCatalog.get_or_create_from_request(reqStruct(query_params={}, data=request))

    assert req_collide.checksum == checksum
    assert req.checksum != checksum
