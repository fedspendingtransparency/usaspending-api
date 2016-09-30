from django.test import TestCase, Client
from usaspending_api.awards.models import Award
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class ContractsLoadTests(TestCase):

    fixtures = ['agencies']

    @pytest.mark.django_db
    def test_contract_load(self):
        """
        Ensure contract awards can be loaded from usaspending
        """
        call_command('loadcontracts', os.path.join(settings.BASE_DIR, 'usaspending_api/data/usaspending_treasury_contracts.csv'))

    def teardown():
        Award.objects.all().delete()
