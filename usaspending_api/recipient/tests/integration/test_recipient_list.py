# Stdlib imports
import datetime
import pytest
# Core Django imports

# Third-party app imports
from rest_framework import status
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
def test_one_recipient():
    """Verify error on bad autocomplete request for budget function."""
    mommy.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="WILSON AND ASSOC",
        last_12_months=-29470313.00,
    )

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount"}
    results, meta = get_recipients(filters=filters)
    assert meta["total"] == 1


@pytest.mark.django_db
def test_ignore_special_case():
    """Verify error on bad autocomplete request for budget function."""
    mommy.make(
        RecipientProfile,
        recipient_level="R",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b85",
        recipient_unique_id=None,
        recipient_name="MULTIPLE RECIPIENTS",
        last_12_months=-29470313.00,
    )

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount"}
    results, meta = get_recipients(filters=filters)
    assert meta["total"] == 0


@pytest.mark.django_db
def test_filters_with_two_recipients():
    """Verify error on bad autocomplete request for budget function."""
    mommy.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="WILSON AND ASSOC",
        last_12_months=-29470313.00,
    )
    mommy.make(
        RecipientProfile,
        recipient_level="B",
        recipient_hash="c8f79139-38b2-3063-b039-d48172abc710",
        recipient_unique_id="000000444",
        recipient_name="DREW JORDAN INC.",
        last_12_months=99705.97,
    ),

    filters = {"limit": 1, "page": 1, "order": "desc", "sort": "amount"}
    results, meta = get_recipients(filters=filters)
    # Ensure pagination metadata meets API Contract
    assert meta["total"] == 2
    assert meta["page"] == 1
    assert meta["limit"] == 1
    assert len(results) == 1
    assert results[0]['recipient_level'] == "B"
    assert float(results[0]['amount']) == float(99705.97)
    assert results[0]['id'] == "c8f79139-38b2-3063-b039-d48172abc710-B"

    filters = {"limit": 1, "page": 1, "order": "asc", "sort": "amount"}
    results, meta = get_recipients(filters=filters)
    assert results[0]['recipient_level'] == "A"

    filters = {"limit": 10, "page": 1, "order": "asc", "sort": "amount", "keyword": "JOR"}
    results, meta = get_recipients(filters=filters)
    assert len(results) == 1
    assert results[0]['recipient_level'] == "B"


@pytest.mark.django_db
def test_state_metadata_with_no_results(client):

    resp = client.post(list_recipients_endpoint())
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data.get('page_metadata', False)
    assert resp.data['page_metadata'].get('next', True) is None
