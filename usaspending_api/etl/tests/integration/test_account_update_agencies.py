import pytest

from model_bakery import baker
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.etl.operations.federal_account.update_agency import update_federal_account_agency
from usaspending_api.etl.operations.treasury_appropriation_account.update_agencies import (
    update_treasury_appropriation_account_agencies,
)


@pytest.fixture
def data_fixture(db):
    # CGAC agency
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=1,
        allocation_transfer_agency_id="123",
        agency_id="123",
        main_account_code="0111",
        fr_entity_code="2345",
    )

    # One more CGAC agency so we can test a different mapping
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=2,
        allocation_transfer_agency_id=None,
        agency_id="234",
        main_account_code="0222",
        fr_entity_code="3456",
    )

    # Shared FREC agency
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=3,
        allocation_transfer_agency_id=None,
        agency_id="345",
        main_account_code="0333",
        fr_entity_code="4567",
    )

    # A few agencies that are shared non-FREC agencies so we can test grouping/counting.
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=4,
        allocation_transfer_agency_id=None,
        agency_id="345",
        main_account_code="0444",
        fr_entity_code="5678",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=5,
        allocation_transfer_agency_id=None,
        agency_id="345",
        main_account_code="0444",
        fr_entity_code="6789",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        pk=6,
        allocation_transfer_agency_id=None,
        agency_id="345",
        main_account_code="0444",
        fr_entity_code="6789",
    )

    # Stuff required to support the above.
    baker.make("accounts.FederalAccount", pk=1, agency_identifier="123", main_account_code="0111")
    baker.make("accounts.FederalAccount", pk=2, agency_identifier="234", main_account_code="0222")
    baker.make("accounts.FederalAccount", pk=3, agency_identifier="345", main_account_code="0333")
    baker.make("accounts.FederalAccount", pk=4, agency_identifier="345", main_account_code="0444")

    baker.make("references.FREC", frec_code="2345", associated_cgac_code="123")
    baker.make("references.FREC", frec_code="3456", associated_cgac_code="234")
    baker.make("references.FREC", frec_code="4567", associated_cgac_code="234")
    baker.make("references.FREC", frec_code="5678", associated_cgac_code="123")
    baker.make("references.FREC", frec_code="6789", associated_cgac_code="234")

    baker.make("references.ToptierAgency", pk=1, toptier_code="123", _fill_optional=True)  # CGAC agency
    baker.make("references.ToptierAgency", pk=2, toptier_code="234", _fill_optional=True)  # Another CGAC agency
    baker.make("references.ToptierAgency", pk=3, toptier_code="4567", _fill_optional=True)  # FREC agency


def test_federal_account_update_agency(data_fixture):

    # Make sure our federal and treasury accounts are unlinked.
    assert TreasuryAppropriationAccount.objects.filter(awarding_toptier_agency__isnull=True).count() == 6
    assert TreasuryAppropriationAccount.objects.filter(funding_toptier_agency__isnull=True).count() == 6
    assert FederalAccount.objects.filter(parent_toptier_agency__isnull=True).count() == 4

    update_federal_account_agency()
    update_treasury_appropriation_account_agencies()

    assert TreasuryAppropriationAccount.objects.get(pk=1).awarding_toptier_agency_id == 1
    assert TreasuryAppropriationAccount.objects.get(pk=1).funding_toptier_agency_id == 1
    assert TreasuryAppropriationAccount.objects.get(pk=2).awarding_toptier_agency_id is None
    assert TreasuryAppropriationAccount.objects.get(pk=2).funding_toptier_agency_id == 2
    assert TreasuryAppropriationAccount.objects.get(pk=3).funding_toptier_agency_id == 3
    assert TreasuryAppropriationAccount.objects.get(pk=4).funding_toptier_agency_id == 2
    assert TreasuryAppropriationAccount.objects.get(pk=5).funding_toptier_agency_id == 2
    assert TreasuryAppropriationAccount.objects.get(pk=6).funding_toptier_agency_id == 2

    assert FederalAccount.objects.get(pk=1).parent_toptier_agency_id == 1
    assert FederalAccount.objects.get(pk=2).parent_toptier_agency_id == 2
    assert FederalAccount.objects.get(pk=3).parent_toptier_agency_id == 3
    assert FederalAccount.objects.get(pk=4).parent_toptier_agency_id == 2
