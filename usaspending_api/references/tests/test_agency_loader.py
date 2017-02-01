import pytest

from django.core.management import call_command

from usaspending_api.references.models import Agency


@pytest.fixture()
def agency_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')
    call_command('loadagencies')


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
