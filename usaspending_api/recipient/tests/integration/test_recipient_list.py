# Stdlib imports
import datetime
import pytest

# Core Django imports

# Third-party app imports
from rest_framework import status
from model_bakery import baker

# Imports from your apps
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.recipient.v2.views.list_recipients import get_recipients

# Getting relative dates as the 'latest'/default argument returns results relative to when it gets called
TODAY = datetime.datetime.now()
OUTSIDE_OF_LATEST = TODAY - datetime.timedelta(365 + 2)
CURRENT_FISCAL_YEAR = generate_fiscal_year(TODAY)


def list_recipients_endpoint():
    return "/api/v2/recipient/duns/"


@pytest.mark.django_db
def test_one_recipient():
    """Verify error on bad autocomplete request for budget function."""
    baker.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="WILSON AND ASSOC",
        last_12_months=-29470313.00,
    )

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount", "award_type": "all"}
    results, meta = get_recipients(filters=filters)
    assert meta["total"] == 1


@pytest.mark.django_db
def test_ignore_special_case():
    """Verify error on bad autocomplete request for budget function."""
    baker.make(
        RecipientProfile,
        recipient_level="R",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b85",
        recipient_unique_id=None,
        recipient_name="MULTIPLE RECIPIENTS",
        last_12_months=-29470313.00,
    )

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount", "award_type": "all"}
    results, meta = get_recipients(filters=filters)
    assert meta["total"] == 0


@pytest.mark.django_db
def test_filters_with_two_recipients():
    """Verify error on bad autocomplete request for budget function."""
    baker.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="WILSON AND ASSOC",
        last_12_months=-29470313.00,
    )
    baker.make(
        RecipientProfile,
        recipient_level="B",
        recipient_hash="c8f79139-38b2-3063-b039-d48172abc710",
        recipient_unique_id="000000444",
        recipient_name="DREW JORDAN INC.",
        last_12_months=99705.97,
    ),

    filters = {"limit": 1, "page": 1, "order": "desc", "sort": "amount", "award_type": "all"}
    results, meta = get_recipients(filters=filters)
    # Ensure pagination metadata meets API Contract
    assert meta["total"] == 2
    assert meta["page"] == 1
    assert meta["limit"] == 1
    assert len(results) == 1
    assert results[0]["recipient_level"] == "B"
    assert float(results[0]["amount"]) == float(99705.97)
    assert results[0]["id"] == "c8f79139-38b2-3063-b039-d48172abc710-B"

    filters = {"limit": 1, "page": 1, "order": "asc", "sort": "amount", "award_type": "all"}
    results, meta = get_recipients(filters=filters)
    assert results[0]["recipient_level"] == "A"

    filters = {"limit": 10, "page": 1, "order": "asc", "sort": "amount", "keyword": "JOR", "award_type": "all"}
    results, meta = get_recipients(filters=filters)
    assert len(results) == 1
    assert results[0]["recipient_level"] == "B"


@pytest.mark.django_db
def test_state_metadata_with_no_results(client):

    resp = client.post(list_recipients_endpoint())
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data.get("page_metadata", False)
    assert resp.data["page_metadata"].get("next", True) is None


@pytest.mark.django_db
def test_award_type_filter():
    """Verify error on bad autocomplete request for budget function."""
    baker.make(
        RecipientProfile,
        recipient_level="A",
        recipient_hash="00077a9a-5a70-8919-fd19-330762af6b84",
        recipient_unique_id="000000123",
        recipient_name="SHOES AND SOCKS INC.",
        last_12_months=2400.00,
        last_12_contracts=400.00,
        last_12_grants=500.00,
        last_12_loans=0.00,
        last_12_other=700.00,
        last_12_direct_payments=800.00,
        award_types=["contract", "grant", "direct payment", "other"],
    )
    baker.make(
        RecipientProfile,
        recipient_level="B",
        recipient_hash="c8f79139-38b2-3063-b039-d48172abc710",
        recipient_unique_id="000000444",
        recipient_name="SPORT SHORTS",
        last_12_months=2000.00,
        last_12_contracts=700.00,
        last_12_grants=600.00,
        last_12_loans=0.00,
        last_12_other=400.00,
        last_12_direct_payments=300.00,
        award_types=["contract", "grant", "direct payment", "other"],
    )
    baker.make(
        RecipientProfile,
        recipient_level="C",
        recipient_hash="5770e860-0f7b-69f1-182f-4d6966ebaa62",
        recipient_unique_id="000000555",
        recipient_name="JUST JERSEYS",
        last_12_months=99.99,
        last_12_contracts=0.00,
        last_12_grants=0.00,
        last_12_loans=99.99,
        last_12_other=0.00,
        last_12_direct_payments=0.00,
        award_types=["loans"],
    )

    filters = {"limit": 10, "page": 1, "order": "desc", "sort": "amount", "award_type": "all"}
    results, meta = get_recipients(filters=filters)

    # "all"
    assert len(results) == 3
    assert results[0]["recipient_level"] == "A"
    assert float(results[0]["amount"]) == float(2400)
    assert results[0]["id"] == "00077a9a-5a70-8919-fd19-330762af6b84-A"

    # Test "grants"
    filters["award_type"] = "grants"
    results, meta = get_recipients(filters=filters)
    assert len(results) == 2
    assert results[0]["recipient_level"] == "B"
    assert float(results[0]["amount"]) == float(600)
    assert results[0]["id"] == "c8f79139-38b2-3063-b039-d48172abc710-B"

    # Test "contracts"
    filters["award_type"] = "contracts"
    results, meta = get_recipients(filters=filters)
    assert len(results) == 2
    assert results[0]["recipient_level"] == "B"
    assert float(results[0]["amount"]) == float(700)
    assert results[0]["id"] == "c8f79139-38b2-3063-b039-d48172abc710-B"

    # Test "direct_payments"
    filters["award_type"] = "direct_payments"
    results, meta = get_recipients(filters=filters)
    assert len(results) == 2
    assert results[0]["recipient_level"] == "A"
    assert float(results[0]["amount"]) == float(800)
    assert results[0]["id"] == "00077a9a-5a70-8919-fd19-330762af6b84-A"

    # Test "loans"
    filters["award_type"] = "loans"
    results, meta = get_recipients(filters=filters)
    assert len(results) == 1
    assert results[0]["recipient_level"] == "C"
    assert float(results[0]["amount"]) == float(99.99)
    assert results[0]["id"] == "5770e860-0f7b-69f1-182f-4d6966ebaa62-C"
