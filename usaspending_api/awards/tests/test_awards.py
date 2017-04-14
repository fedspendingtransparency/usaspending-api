import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


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
def test_get_or_create_summary_award():
    """Test award record lookup."""
    sta1 = mommy.make(SubtierAgency, subtier_code='1234', name='Bureau of Effective Unit Tests')
    tta1 = mommy.make(ToptierAgency, cgac_code='020', name='Department of Unit Tests')
    a1 = mommy.make(Agency, id=1, toptier_agency=tta1, subtier_agency=sta1)

    # match on awarding agency and piid
    m1 = mommy.make('awards.award', piid='DUT123', awarding_agency=a1)
    t1 = Award.get_or_create_summary_award(piid='DUT123', awarding_agency=a1)[1]
    assert t1 == m1

    # match on awarding agency and piid + parent award
    pa1 = mommy.make('awards.award', piid='IDVDUT456')
    m2 = mommy.make('awards.award', piid='DUT456', parent_award=pa1, awarding_agency=a1)
    t2 = Award.get_or_create_summary_award(piid='DUT456', parent_award_id='IDVDUT456', awarding_agency=a1)[1]
    assert t2 == m2

    # match on awarding agency and fain
    m3 = mommy.make('awards.award', fain='DUT789', awarding_agency=a1)
    t3 = Award.get_or_create_summary_award(fain='DUT789', awarding_agency=a1)[1]
    assert t3 == m3

    # match on awarding agency and fain + uri (fain takes precedence)
    # m4 = mommy.make('awards.award', fain='DUT789', uri='123-abc-456', awarding_agency=a1)
    # t4 = Award.get_or_create_summary_award(fain='DUT789', uri='123-abc-456', awarding_agency=a1)[1]
    # assert t4 == m4

    # match on awarding agency and fain + uri (fain takes precedence)
    # m5 = mommy.make('awards.award', fain='DUT789', uri='123-abc-456', awarding_agency=a1)
    # t5 = Award.get_or_create_summary_award(fain='DUT789', uri='123-abc-456-a-different-uri', awarding_agency=a1)[1]
    # assert t5 == m5

    # match on awarding agency + uri
    # match on awarding toptier agency only
    # match on no awarding agency
    # non-match with piid creates new award record
    # non-match with piid + non-matching parent award creates two new awards
    # non-match with piid + matching parent award creates one new award
    # matching piid + non-matching parent ???
