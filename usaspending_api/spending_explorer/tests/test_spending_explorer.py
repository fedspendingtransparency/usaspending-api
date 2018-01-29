import json

import pytest
from rest_framework import status


@pytest.mark.django_db
def test_budget_function_filter_success(client):

    # Test for Budget Function Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "budget_function",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Budget Sub Function Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                    "recipient": 13916
                }}))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_budget_function_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_object_class_filter_success(client):

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "agency",
                "filters": {
                    "fy": "2017",
                    "object_class": "20"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017",
                    "object_class": "20"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "federal_account": 2358
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "federal_account": 2358,
                    "program_activity": 15103
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "federal_account": 2358,
                    "program_activity": 15103,
                    "recipient": 301773
                }}))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_object_class_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_agency_filter_success(client):

    # Test for Object Class Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "agency",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {
                    "fy": "2017"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "federal_account": 1500
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "federal_account": 1500,
                    "program_activity": 12697
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40"
                }}))
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        '/api/v2/spending/',
        content_type='application/json',
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917
                }
            }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_agency_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
