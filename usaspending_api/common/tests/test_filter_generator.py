import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.common.api_request_utils import FilterGenerator


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
    filters = [
        {
            "field": "description",
            "operation": "search",
            "value": "small"
        }
    ]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 3


@pytest.mark.django_db
def test_filter_generator_in_operation(client, mock_data):
    """Test in operation case insensitivity"""
    filters = [
        {
            "field": "description",
            "operation": "in",
            "value": ["small business administration", "large business administration"]
        }
    ]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 5

    filters = [
        {
            "field": "description",
            "operation": "not_in",
            "value": ["small business administration", "large"]
        }
    ]

    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 2


@pytest.mark.django_db
def test_filter_generator_equals_operation(client, mock_data):
    """Test equals case insensitivity"""
    filters = [
        {
            "field": "description",
            "operation": "equals",
            "value": "small business administration"
        }
    ]

    fg = FilterGenerator(Award)
    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 3

    filters = [
        {
            "field": "description",
            "operation": "not_equals",
            "value": "small business administration"
        }
    ]

    q_obj = fg.create_q_from_filter_list(filters)

    # Verify the filter returns the appropriate number of matches
    assert Award.objects.filter(q_obj).count() == 2
