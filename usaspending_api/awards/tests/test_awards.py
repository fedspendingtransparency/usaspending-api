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
    assert len(resp.data) > 1

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
    # LEGACY CODE: History is not required for TransactionNormalized, TransactionFPDS, TransactionFABS

    "test that changes to a transaction are logged in the history file"
    # t1 = mommy.make('awards.transactionnormalized', description='bought some stuff', _fill_optional=True,)
    # assert t1.history.count() == 1
    # t1.description = 'Procured mission-critical resources'
    # t1.save()
    # assert t1.history.count() == 2
    # assert t1.history.filter(description='bought some stuff').count() == 1
    # assert t1.history.filter(description='this never happened').count() == 0

    # tc1 = mommy.make('awards.transactionfpds', transaction=t1, current_total_value_award=1000.00)
    # assert tc1.history.count() == 1
    # tc1.base_exercised_options_val = 2000.00
    # tc1.save()
    # assert tc1.history.count() == 2
    # tc1.history.filter(base_exercised_options_val=1000.00).count() == 1
    #
    # t2 = mommy.make('awards.transactionnormalized', description='doled out some dough', _fill_optional=True,)
    # ta2 = mommy.make('awards.transactionfabs', transaction=t2, total_funding_amount=100.00)
    # assert ta2.history.count() == 1
    # ta2.total_funding_amount = 300.00
    # ta2.save()
    # assert ta2.history.count() == 2
    # ta2.history.filter(total_funding_amount=300.00).count() == 1


@pytest.mark.django_db
def test_get_or_create_summary_award():
    """Test award record lookup."""
    sta1 = mommy.make(SubtierAgency, subtier_code='1234', name='Bureau of Effective Unit Tests')
    sta2 = mommy.make(SubtierAgency, subtier_code='5678', name='Bureau of Breaky Unit Tests')
    sta3 = mommy.make(SubtierAgency, subtier_code='0509', name='Bureau of Testing Bureaucracy')
    tta1 = mommy.make(ToptierAgency, cgac_code='020', name='Department of Unit Tests')
    tta2 = mommy.make(ToptierAgency, cgac_code='089', name='Department of Integrated Tests')

    a1 = mommy.make(Agency, id=1, toptier_agency=tta1, subtier_agency=sta1)
    a2 = mommy.make(Agency, id=2, toptier_agency=tta1)
    a3 = mommy.make(Agency, id=3, toptier_agency=tta1, subtier_agency=sta2)
    a4 = mommy.make(Agency, id=4, toptier_agency=tta2, subtier_agency=sta3)

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

    # match on awarding agency and fain + uri (fain takes precedence, same uri)
    m4 = mommy.make('awards.award', fain='DUT987', uri='123-abc-456', awarding_agency=a1)
    t4 = Award.get_or_create_summary_award(fain='DUT987', uri='123-abc-456', awarding_agency=a1)[1]
    assert t4 == m4

    # fain + uri are unique on their own
    m5 = mommy.make('awards.award', fain='DUT123456', uri='123-abc-456', awarding_agency=a1)
    t5 = Award.get_or_create_summary_award(fain='DUT123456', uri='123-abc-456-a-different-uri', awarding_agency=a1)[1]
    assert t5 != m5

    # match on awarding agency + uri
    m6 = mommy.make('awards.award', uri='abc-123-def', awarding_agency=a1)
    t6 = Award.get_or_create_summary_award(uri='abc-123-def', awarding_agency=a1)[1]
    assert t6 == m6

    # match on awarding toptier agency only
    m7 = mommy.make('awards.award', uri='kkk-bbb-jjj', awarding_agency=a2)
    t7 = Award.get_or_create_summary_award(uri='kkk-bbb-jjj', awarding_agency=a2)[1]
    assert m7 == t7

    # match on no awarding agency, uri
    m8 = mommy.make('awards.award', uri='mmm-fff-ddd')
    t8 = Award.get_or_create_summary_award(uri='mmm-fff-ddd')[1]
    assert m8 == t8

    # match on no awarding agency, fain
    m9 = mommy.make('awards.award', fain='DUTDUTDUT')
    t9 = Award.get_or_create_summary_award(fain='DUTDUTDUT')[1]
    assert m9 == t9

    # non-match with piid creates new award record
    m10 = mommy.make('awards.award', piid='imapiid')
    t10 = Award.get_or_create_summary_award(piid='imadifferentpiid')
    assert len(t10[0]) == 1
    assert m10 != t10[1]

    # non-match with piid + non-matching parent award creates two new awards
    pa11 = mommy.make('awards.award', piid='momofpiidsarefun')
    m11 = mommy.make('awards.award', piid='piidsarefun', parent_award=pa11)
    t11 = Award.get_or_create_summary_award(piid='piidsarefun', parent_award_id='dadofpiidsarefun')
    assert len(t11[0]) == 2
    assert m11 != t11[1]

    # non-match with piid + matching parent award creates one new award
    pa12 = mommy.make('awards.award', piid='thingmom')
    m12 = mommy.make('awards.award', piid='thing1', parent_award=pa12)
    t12 = Award.get_or_create_summary_award(piid='thing2', parent_award_id='thingmom')
    assert len(t12[0]) == 1
    assert m12 != t12[1]

    # matching piid + non-matching parent
    pa13 = mommy.make('awards.award', piid='piidthing')
    m13 = mommy.make('awards.award', piid='0005', parent_award=pa13)
    t13 = Award.get_or_create_summary_award(piid='0005', parent_award_id='anotherpiidthing')[1]
    assert t13 != m13

    # matching piid and parent award id but mismatched subtier agency, same top tier agency
    pa14 = mommy.make('awards.award', piid='imalittlepiid', awarding_agency=a1)
    m14 = mommy.make('awards.award', piid='shortandstout', parent_award=pa14, awarding_agency=a1)
    t14 = Award.get_or_create_summary_award(piid='shortandstout', parent_award_id='imalittlepiid', awarding_agency=a3)[1]
    assert t14 != m14

    # matching fain but mismatched subtier agency, same top tier agency
    m15 = mommy.make('awards.award', fain='imalittlefain', parent_award=pa14, awarding_agency=a1)
    t15 = Award.get_or_create_summary_award(fain='imalittlefain', awarding_agency=a3)[1]
    assert t15 != m15

    # matching piid and parent award but mismatched subtier and toptier agency
    pa16 = mommy.make('awards.Award', piid='imjustapiidparent', awarding_agency=a3)
    m16 = mommy.make('awards.Award', piid='imjustapiidchild', awarding_agency=a3)
    t16 = Award.get_or_create_summary_award(piid='imjustapiidchild', parent_award_id='imjustapiidparent', awarding_agency=a4)[1]
    assert t16 != m16


@pytest.mark.django_db
def test_cfda_objectives():
    """Verify that cfda_objectives property works."""

    cfda = mommy.make('references.Cfda', program_number='1.001', objectives='a brighter tomorrow')
    tfabs = mommy.make('awards.TransactionFABS', cfda_number='1.001')
    assert tfabs.cfda_objectives == 'a brighter tomorrow'
    tfabs2 = mommy.make('awards.TransactionFABS', cfda_number='1.001xxx')
    assert tfabs2.cfda_objectives is None
