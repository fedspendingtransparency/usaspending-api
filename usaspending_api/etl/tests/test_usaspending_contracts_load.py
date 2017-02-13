import os

from django.conf import settings
from django.core.management import call_command
import pytest

from usaspending_api.etl.management.commands import load_usaspending_contracts


# Transaction test cases so threads can find the data
@pytest.mark.django_db(transaction=True)
def test_contract_load():
    """Ensure contract awards can be loaded from usaspending"""
    call_command('loaddata', 'endpoint_fixture_db')
    call_command('load_usaspending_contracts',
                 os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                              'usaspending_treasury_contracts.csv'))
    # @todo - should there be an assert here?


@pytest.mark.parametrize(
    'contract_type,expected',
    [
        ('a b c d', 'a'),  # splits and cares only about 1 char
        ('a:b:c d:e:f g', 'a'),
        ('something-BPA related', 'A'),
        ('something with pUrCHases', 'B'),
        ('delivery:aaa definitive:bbb', 'C'),  # check in order
        ('definitive', 'D'),
        ('something else entirely', None)
    ])
def test_evaluate_contract_award_type(contract_type, expected):
    """Verify that contract types are converted correctly"""
    row = dict(contractactiontype=contract_type)
    result = load_usaspending_contracts.evaluate_contract_award_type(row)
    assert result == expected
