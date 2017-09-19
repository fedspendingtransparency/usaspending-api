import os

from django.conf import settings
from django.core.management import call_command
import pytest

from usaspending_api.etl.management.commands import load_usaspending_assistance
from usaspending_api.etl.tests.test_helpers import mutated_csv
from usaspending_api.awards.models import Award
from usaspending_api.broker.models import TransactionNormalized
from usaspending_api.references.models import Agency


# Transaction test cases so threads can find the data
@pytest.mark.django_db(transaction=True)
def test_usaspending_assistance_load():
    """Ensure assistance awards can be loaded from usaspending"""
    call_command('loaddata', 'endpoint_fixture_db')
    call_command('load_usaspending_assistance',
                 os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                              'usaspending_fin_assist_direct_payments.csv'))

    # @todo - should there be an assert here?


@pytest.mark.django_db(transaction=True)
def test_award_and_txn_uniqueness():
    """Each Award should be unique by:
    - PIID (Contract) or FAIN/URI (Grants)
    - Parent Award (but grants don't have parents)
    - Awarding sub-tier Agency

    Each Transaction should be unique by
    - Award
    - Modification number"""
    call_command('loaddata', 'endpoint_fixture_db')

    # agency records from reference_fixture are a prereq

    awards_before_loads = Award.objects.count()
    txn_before_loads = Transaction.objects.count()
    call_command('load_agencies')
    filepath = os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                            'usaspending_fin_assist_direct_payments.csv')
    call_command('load_usaspending_assistance', filepath)

    # we record the # of records created by uploading the CSV
    # in awards_in_csv and txn_in_csv
    awards_in_csv = Award.objects.count() - awards_before_loads
    txn_in_csv = Transaction.objects.count() - txn_before_loads

    def new_fain(row):
        row['federal_award_id'] = '{}-fain2'.format(row['federal_award_id'])
        return row

    awards_before_test = Award.objects.count()

    # Load the same test data, but this time giving each row a new
    # FAIN, to verify that the new FAINs produce new records
    with mutated_csv(filepath, new_fain) as mutant_file:
        call_command('load_usaspending_assistance', mutant_file.name)

    # Verify that each row created a new Award
    assert Award.objects.count() == awards_before_test + awards_in_csv

    # Next we verify that changing awarding agency creates
    # new awards.  We do this by assigning existing Awards
    # to a different awarding agency, then re-importing the
    # original CSV (unchanged).

    # Find an agency with no awards
    agencies_in_use = Award.objects.values('awarding_agency__id')
    new_agency = Agency.objects.exclude(id__in=agencies_in_use).first()
    # Assign loaded records to that agency
    for award in Award.objects.all():
        award.awarding_agency = new_agency
        award.save()
    # re-import original file - should have new records
    awards_before_test = Award.objects.count()
    call_command('load_usaspending_assistance', filepath)
    assert Award.objects.count() == awards_before_test + awards_in_csv

    # changing the modification number should create new transaction
    # but not new award

    def increase_mod_num(row):
        current_mod = int(row['federal_award_mod'] or 0)
        row['federal_award_mod'] = current_mod + 1000
        return row

    awards_before_test = Award.objects.count()
    txn_before_test = Transaction.objects.count()

    # Loading the original CSV again, but with a different
    # modification number for each row
    with mutated_csv(filepath, increase_mod_num) as mutant_file:
        call_command('load_usaspending_assistance', mutant_file.name)
    assert Award.objects.count() == awards_before_test
    assert Transaction.objects.count() == txn_before_test + txn_in_csv


@pytest.mark.parametrize('row,expected', [
    ({
        'principal_place_zip': '12345-6789'
    }, {
        'location_zip': '123456789'
    }),
    ({
        'principal_place_state_code': 'OH'
    }, {
        'state_code': 'OH'
    }),
    ({
        'principal_place_cc': 'MONTGOMERY'
    }, {
        'county_name': 'MONTGOMERY'
    }),
])
def test_location_mapper_fin_assistance_principal_place(row, expected):
    """Verify that principal place data translated to location fields"""
    result = load_usaspending_assistance.location_mapper_fin_assistance_principal_place(
        row)
    for key in expected:
        assert key in result
        assert result[key] == expected[key]


@pytest.mark.parametrize('row,expected', [
    ({
        'recipient_zip': '12345-6789'
    }, {
        'location_zip': '123456789'
    }),
    ({
        'recipient_state_code': 'OH'
    }, {
        'state_code': 'OH'
    }),
    ({
        'recipient_county_name': 'MONTGOMERY'
    }, {
        'county_name': 'MONTGOMERY'
    }),
    ({
        'receip_addr1': '123 E Main'
    }, {
        'address_line1': '123 E Main'
    }),
])
def test_location_mapper_fin_assistance_recipient(row, expected):
    """Verify that recipient place data translated to location fields"""
    result = load_usaspending_assistance.location_mapper_fin_assistance_recipient(
        row)
    for key in expected:
        assert key in result
        assert result[key] == expected[key]
