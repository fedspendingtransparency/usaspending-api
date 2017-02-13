
from django.conf import settings
import pytest
from model_mommy import mommy

from usaspending_api.references.models import Agency, OfficeAgency, SubtierAgency, ToptierAgency


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up a data_broker test db, remove it
    # from the list of databases in django settings
    settings.DATABASES.pop('data_broker', None)


@pytest.fixture(scope='session')
def agencies():
    """Setup agency hierarchy for use in tests."""
    o = mommy.make(OfficeAgency, aac_code='aac1', name='The Office')
    s = mommy.make(SubtierAgency, subtier_code='sub1', name='Subtiers of a Clown')
    t = mommy.make(ToptierAgency, cgac_code='cgac1', name='Department of Test Data Naming')
    mommy.make(Agency, id=1, toptier_agency=t, subtier_agency=s, office_agency=o)
    o = mommy.make(OfficeAgency, aac_code='aac2', name='Office Space')
    s = mommy.make(SubtierAgency, subtier_code='sub2', name='Subtiers in my Beers')
    t = mommy.make(ToptierAgency, cgac_code='cgac2', name='Department of Bureacracy')
    mommy.make(Agency, id=2, toptier_agency=t, subtier_agency=s, office_agency=o)
