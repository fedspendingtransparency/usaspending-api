import datetime

from model_mommy import mommy
import pytest

from django.core.management import call_command

from usaspending_api.references.models import Agency


@pytest.fixture()
def agency_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')
    call_command('load_agencies')


@pytest.mark.django_db
def test_department(agency_data):
    """
    Make sure an instance of a department is properly created
    """

    Agency.objects.get(
        toptier_agency__cgac_code='002',
        toptier_agency__fpds_code='0000',
        subtier_agency__subtier_code='0000')


@pytest.mark.django_db
def test_subtier(agency_data):
    """
    Make sure a subtier is properly mapped to its parent department
    """

    subtier = Agency.objects.get(toptier_agency__cgac_code='002',
                                 toptier_agency__fpds_code='0000',
                                 subtier_agency__subtier_code='0001')
    department = Agency.objects.get(toptier_agency__cgac_code='002',
                                    toptier_agency__fpds_code='0000',
                                    subtier_agency__subtier_code='0000')
    assert subtier.toptier_agency == department.toptier_agency


@pytest.mark.django_db
def test_get_by_toptier():
    """Test Agency lookup by toptier CGAC code."""
    toptier = mommy.make('references.ToptierAgency', cgac_code='xyz', name='yo')
    subtier = mommy.make('references.SubtierAgency', subtier_code='abc', name='yo')
    mommy.make(
        'references.Agency', toptier_agency=toptier, subtier_agency=subtier)
    mommy.make(
        'references.Agency',
        toptier_agency=toptier,
        subtier_agency=mommy.make('references.SubtierAgency', subtier_code='abc', name='no'),
        update_date=datetime.date(2017, 10, 10))
    agency1 = mommy.make(
        'references.Agency', toptier_agency=toptier, subtier_agency=subtier)

    # lookup should return agency w/ most recent update_date that
    # matches the cgac code
    assert Agency.get_by_toptier('xyz') == agency1
    # If there's no match, we should get none
    assert Agency.get_by_toptier('nope') is None


@pytest.mark.django_db
def test_get_by_subtier():
    """Test Agency lookup by subtier."""
    toptier = mommy.make('references.ToptierAgency', cgac_code='xyz', name='yo')
    subtier = mommy.make('references.SubtierAgency', subtier_code='abc', name='hi')
    mommy.make(
        'references.Agency', toptier_agency=toptier, subtier_agency=subtier)
    mommy.make(
        'references.Agency',
        toptier_agency=toptier,
        subtier_agency=mommy.make('references.SubtierAgency', subtier_code='bbb'))
    agency1 = mommy.make(
        'references.Agency', toptier_agency=toptier, subtier_agency=subtier)

    # lookup should return agency w/ most recent updatea_date that
    # matches the subtier code
    assert Agency.get_by_subtier('abc') == agency1
    # if there's no match, we should get none
    assert Agency.get_by_subtier('nope') is None
    # if called with an empty argument, we should get None
    assert Agency.get_by_subtier('') is None
    assert Agency.get_by_subtier(None) is None


@pytest.mark.django_db
def test_get_by_toptier_subtier():
    """Test Agency lookup by subtier."""
    toptier = mommy.make('references.ToptierAgency', cgac_code='xyz', name='yo')
    subtier = mommy.make('references.SubtierAgency', subtier_code='abc', name='hi')

    mommy.make(
        'references.Agency',
        toptier_agency=toptier, subtier_agency=subtier)
    mommy.make(
        'references.Agency',
        toptier_agency=toptier,
        subtier_agency=mommy.make('references.SubtierAgency', subtier_code='bbb'))
    agency1 = mommy.make(
        'references.Agency', toptier_agency=toptier, subtier_agency=subtier)

    # lookup should return agency w/ most recent updatea_date that
    # matches the toptier and subtier code
    assert Agency.get_by_toptier_subtier('xyz', 'abc') == agency1
    # if there's no match, we should get none
    assert Agency.get_by_toptier_subtier('nope', 'nada') is None
    assert Agency.get_by_toptier_subtier('xyz', 'nada') is None
    assert Agency.get_by_toptier_subtier('nope', 'bbb') is None
