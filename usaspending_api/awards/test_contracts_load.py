from django.test import TransactionTestCase, Client
from usaspending_api.awards.models import Award
from django.core.management import call_command
from django.conf import settings
import os
import pytest

from model_mommy import mommy


# Transaction test cases so threads can find the data
class ContractsLoadTests(TransactionTestCase):

    fixtures = ['agencies']

    @pytest.mark.django_db
    def test_contract_load(self):
        """
        Ensure contract awards can be loaded from usaspending
        """
        self.award = mommy.make('submissions.SubmissionProcess', _quantity=2)
        call_command('loadcontracts', os.path.join(settings.BASE_DIR, 'usaspending_api/data/usaspending_treasury_contracts.csv'))

    def teardown():
        Award.objects.all().delete()
