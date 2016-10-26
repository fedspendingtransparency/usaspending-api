from django.test import TestCase, Client
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class TreasuryAppropriationAccountLoadTests(TestCase):

    #fixtures = ['tas']

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

        call_command('loadtas')
        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='00110100', beginning_period_of_availability='2011', ending_period_of_availability='2011')
