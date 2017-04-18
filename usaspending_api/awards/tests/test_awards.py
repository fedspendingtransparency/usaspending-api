import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import (
    Award, Transaction, TransactionContract, TransactionAssistance)


@pytest.mark.django_db
def test_award_endpoint(client):
    """Test the awards endpoint."""

    resp = client.get('/api/v1/awards/')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 2

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json').status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "equals",
                "value": "0300"
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "combine_method": "OR",
                "filters": [{
                    "field": "funding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }, {
                    "field": "awarding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }]
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "ff",
                "value": "0300"
            }]
        })).status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_null_awards():
    """Test the award.nonempty command."""
    mommy.make('awards.Award', total_obligation="2000", _quantity=2)
    mommy.make(
        'awards.Award',
        type="U",
        total_obligation=None,
        date_signed=None,
        recipient=None)

    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2


@pytest.fixture
def awards_data(db):
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000)
    mommy.make(
        'awards.Award',
        piid='###',
        fain='ABC789',
        type='B',
        total_obligation=1000)
    mommy.make('awards.Award', fain='XYZ789', type='C', total_obligation=1000)


def test_award_total_grouped(client, awards_data):
    """Test award total endpoint with a group parameter."""

    resp = client.post(
        '/api/v1/awards/total/',
        content_type='application/json',
        data=json.dumps({
            'field': 'total_obligation',
            'group': 'type',
            'aggregate': 'sum'
        }))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    # our data has two different type codes, we should get two summarized items back
    assert len(results) == 2
    # check total
    for result in resp.data['results']:
        if result['item'] == 'B':
            assert float(result['aggregate']) == 2000
        else:
            assert float(result['aggregate']) == 1000


@pytest.mark.django_db
def test_award_date_signed_fy(client):
    """Test date_signed__fy present and working properly"""

    mommy.make('awards.Award', type='B', date_signed=date(2012, 3, 1))
    mommy.make('awards.Award', type='B', date_signed=date(2012, 11, 1))
    mommy.make('awards.Award', type='C', date_signed=date(2013, 3, 1))
    mommy.make('awards.Award', type='C')

    resp = client.post(
        '/api/v1/awards/?date_signed__fy__gt=2012',
        content_type='application/json')
    results = resp.data['results']
    assert len(results) == 2
    # check total
    for result in resp.data['results']:
        assert 'date_signed__fy' in result
        assert int(result['date_signed__fy']) > 2012


@pytest.mark.django_db
def test_manual_hash_eq_fain():
    """test that records with equal FAIN hash as equal"""
    m1 = mommy.make('awards.award', fain='ABC', piid=None, uri=None, _fill_optional=True)
    m2 = mommy.make('awards.award', fain='ABC', piid=None, uri=None, _fill_optional=True)
    assert m1.manual_hash() == m2.manual_hash()


@pytest.mark.django_db
def test_award_hash_ineq_fain():
    """test that records with unequal FAIN hash as unequal"""
    m1 = mommy.make('awards.award', fain='ABC', piid=None, uri=None, _fill_optional=True)
    m2 = mommy.make('awards.award', fain='XYZ', piid=None, uri=None, _fill_optional=True)
    assert m1.manual_hash() != m2.manual_hash()


@pytest.mark.django_db
def test_transaction_changes_logged():
    "test that changes to a transaction are logged in the history file"
    t1 = mommy.make('awards.transaction', description='bought some stuff', _fill_optional=True,)
    assert t1.history.count() == 1
    t1.description = 'Procured mission-critical resources'
    t1.save()
    assert t1.history.count() == 2
    assert t1.history.filter(description='bought some stuff').count() == 1
    assert t1.history.filter(description='this never happened').count() == 0

    tc1 = mommy.make('awards.transactioncontract', transaction=t1, current_total_value_award=1000.00)
    assert tc1.history.count() == 1
    tc1.current_total_value_award = 2000.00
    tc1.save()
    assert tc1.history.count() == 2
    tc1.history.filter(current_total_value_award=1000.00).count() == 1

    t2 = mommy.make('awards.transaction', description='doled out some dough', _fill_optional=True,)
    ta2 = mommy.make('awards.transactionassistance', transaction=t2, total_funding_amount=100.00)
    assert ta2.history.count() == 1
    ta2.total_funding_amount = 300.00
    ta2.save()
    assert ta2.history.count() == 2
    ta2.history.filter(total_funding_amount=300.00).count() == 1
