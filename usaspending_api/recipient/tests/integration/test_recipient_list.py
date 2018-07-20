# Stdlib imports
import datetime
import pytest
# Core Django imports

# Third-party app imports
# from rest_framework import status
from model_mommy import mommy

# Imports from your apps
# from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
# from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import generate_fiscal_year
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.recipient.v2.views.list_recipients import get_recipients
# Getting relative dates as the 'latest'/default argument returns results relative to when it gets called
TODAY = datetime.datetime.now()
OUTSIDE_OF_LATEST = (TODAY - datetime.timedelta(365 + 2))
CURRENT_FISCAL_YEAR = generate_fiscal_year(TODAY)


def list_recipients_endpoint():
    return '/api/v2/recipient/duns/'


@pytest.mark.django_db
def test_state_metadata_failure():
    """Verify error on bad autocomplete request for budget function."""
    mommy.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="WILSON AND ASSOC",
        last_12_months=-29470313.00,
    ),

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount"}
    results, meta = get_recipients(filters=filters)
    assert meta["total"] == 1
