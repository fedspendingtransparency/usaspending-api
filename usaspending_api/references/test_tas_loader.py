from django.test import TransactionTestCase, Client
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class TreasuryAppropriationAccountLoadTests(TransactionTestCase):

    # fixtures = ['tas']

    @pytest.mark.django_db
#    def test_tas_load(self):
#        """
#        Ensure tas can be loaded from source file
#        """
#        call_command('loadtas')
    def test_gwa_tas(self):
        """
        Make sure an instance of a tas is properly created
        """
        call_command('loadtas', os.path.join(settings.BASE_DIR, 'usaspending_api/data/tas_list.csv'))
        gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier='7')
