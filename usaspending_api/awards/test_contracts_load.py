import os

from django.conf import settings
from django.core.management import call_command
from django.test import TransactionTestCase, Client
from model_mommy import mommy
import pytest

from usaspending_api.awards.models import Award
from usaspending_api.awards.management.commands import loadcontracts


# Transaction test cases so threads can find the data
class ContractsLoadTests(TransactionTestCase):

    fixtures = ['endpoint_fixture_db']

    @pytest.mark.django_db
    def test_contract_load(self):
        """
        Ensure contract awards can be loaded from usaspending
        """
        call_command('loadcontracts', os.path.join(settings.BASE_DIR, 'usaspending_api/data/usaspending_treasury_contracts.csv'))

    def teardown():
        Award.objects.all().delete()


@pytest.mark.parametrize('contract_type,expected', [
    ('a b c d', 'a'),   # splits and cares only about 1 char
    ('a:b:c d:e:f g', 'a'),
    ('something-BPA related', 'A'),
    ('something with pUrCHases', 'B'),
    ('delivery:aaa definitive:bbb', 'C'),   # check in order
    ('definitive', 'D'),
    ('something else entirely', None)
])
def test_evaluate_contract_award_type(contract_type, expected):
    """Verify that contract types are converted correctly"""
    command = loadcontracts.Command()
    row = dict(contractactiontype=contract_type)
    result = command.evaluate_contract_award_type(row)
    assert result == expected
