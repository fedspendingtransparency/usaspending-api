from usaspending_api.accounts.models import TreasuryAppropriationAccount
from django.core.management import call_command
from django.conf import settings
import os
import pytest

#    def test_tas_load(self):
#        """
#        Ensure tas can be loaded from source file
#        """
#        call_command('loadtas')


@pytest.fixture()
def tas_data():
    call_command('loadtas',
                 os.path.join(settings.BASE_DIR,
                              'usaspending_api/data/tas_list.csv'))
    mommy.make('submissions.SubmissionAttributes', _quantity=2)


@pytest.mark.django_db
def test_gwa_tas(tas_data):
    """
    Make sure an instance of a tas is properly created
    """
    gwa_tas = TreasuryAppropriationAccount.objects.get(
        treasury_account_identifier='7')
