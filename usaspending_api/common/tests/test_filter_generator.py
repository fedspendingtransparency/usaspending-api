import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award

@pytest.fixture
def mock_data():
    """mock data"""
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
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
        fain='abc123',
        type='B',
        total_obligation=1000,
        description='LARGE BUSINESS ADMINISTRATION',
        date_signed=date(2012, 3, 1))


@pytest.mark.django_db
def test_filter_generator_search_operation(client, mock_data):
    """Test search case insensitivity"""
    resp = client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "description",
                "operation": "search",
                "value": "small"
            }]
        }))
    results = resp.data['results']
    assert len(results) == 3


@pytest.mark.django_db
def test_filter_generator_in_operation(client, mock_data):
    """Test in operation case insensitivity"""
    # 'in' operation
    resp_a = client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "description",
                "operation": "in",
                "value": ["small business administration", "large business administration"]
            }]
        }))
    results_a = resp_a.data['results']
    assert len(results_a) == 5

    # 'not in' operation
    resp_b = client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "description",
                "operation": "not in",
                "value": ["small business administration", "large"]
            }]
        }))
    results_b = resp_b.data['results']
    assert len(results_b) == 2


@pytest.mark.django_db
def test_filter_generator_equals_operation(client, mock_data):
    """Test equals case insensitivity"""
    # 'equals'
    resp_a = client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "description",
                "operation": "equals",
                "value": "small business administration"
            }]
        }))
    results_a = resp_a.data['results']
    print(resp_a.data['results'])
    assert len(results_a) == 3

    # 'not equals'
    resp_b = client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "description",
                "operation": "not equals",
                "value": "small business administration"
            }]
        }))
    results_b = resp_b.data['results']
    assert len(results_b) == 2
