import os

from django.conf import settings
from django.core.management import call_command
import pytest

from usaspending_api.awards.management.commands import loadfinancialassistance


# Transaction test cases so threads can find the data
@pytest.mark.django_db(transaction=True)
def test_financialassistance_load():
    """Ensure contract awards can be loaded from usaspending"""
    call_command('loaddata', 'endpoint_fixture_db')
    call_command('loadcontracts',
                 os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                              'usaspending_fin_assist_direct_payments.csv'))

    # @todo - should there be an assert here?


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
    result = loadfinancialassistance.location_mapper_fin_assistance_principal_place(
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
    result = loadfinancialassistance.location_mapper_fin_assistance_recipient(
        row)
    for key in expected:
        assert key in result
        assert result[key] == expected[key]
