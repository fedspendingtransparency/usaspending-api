import os

from django.core.management import call_command
from django.conf import settings
import pytest

from usaspending_api.accounts.models import TreasuryAppropriationAccount

tas_test_file = os.path.join(
    settings.BASE_DIR,
    'usaspending_api/data/testing_data/tas_list_1.csv'
)


@pytest.fixture()
def tas_data():
    call_command('loadtas', tas_test_file)


@pytest.mark.django_db(transaction=True)
def test_gwa_tas(tas_data):
    """
    Make sure an instance of a tas is properly created
    """
    gwa_tas = TreasuryAppropriationAccount.objects.get(
        treasury_account_identifier='7')
