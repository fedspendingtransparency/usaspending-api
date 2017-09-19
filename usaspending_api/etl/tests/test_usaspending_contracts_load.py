import os

from django.conf import settings
from django.core.management import call_command
from usaspending_api.awards.models import Award
from usaspending_api.awards.models import TransactionNormalized
import pytest

from usaspending_api.etl.management.commands import load_usaspending_contracts


# Transaction test cases so threads can find the data
@pytest.mark.django_db(transaction=True)
def test_contract_load():
    """ensure contract awards can be loaded from usaspending"""
    call_command('loaddata', 'endpoint_fixture_db')
    # agency records from reference_fixture are a prereq
    call_command('load_agencies')
    call_command('load_usaspending_contracts',
                 os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                              'usaspending_treasury_contracts.csv'))

    # Test that historic USAspending award/contract fields are being mapped
    # correctly to DATA Act schema
    # TODO: fill this out a bit
    assert TransactionNormalized.objects.filter(action_type__isnull=False).count() > 0


@pytest.mark.django_db(transaction=True)
def test_award_and_txn_uniqueness():
    """Each Award should be unique by:
    - PIID (Contract) or FAIN/URI (Grants)
    - Parent Award
    - Awarding sub-tier Agency

    Each Transaction should be unique by
    - Award
    - Modification number"""

    # agency records from reference_fixture are a prereq
    call_command('load_agencies')
    call_command('load_usaspending_contracts',
                 os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                              'usaspending_treasury_contracts.csv'))

    # Each award is unique on these three attributes
    attribs = ('piid', 'parent_award', 'awarding_agency')
    distincts = Award.objects.values(*attribs).distinct().count()
    assert distincts == Award.objects.count()

    # They are not unique individually
    for attrib in attribs:
        less_distinct = Award.objects.values(attrib).distinct().count()
        assert less_distinct < Award.objects.count()

    # Each transaction is unique on award + mod number
    attribs = ('award', 'modification_number')
    distincts = TransactionNormalized.objects.values(*attribs).distinct().count()
    assert distincts == TransactionNormalized.objects.count()

    # But not unique simply on award
    less_distinct = TransactionNormalized.objects.values('award').distinct().count()
    assert less_distinct < TransactionNormalized.objects.count()


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
