import pytest

from django.conf import settings
from django.core.management import call_command
from model_bakery import baker
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.references.account_helpers import (
    insert_federal_accounts,
    link_treasury_accounts_to_federal_accounts,
    remove_empty_federal_accounts,
    update_federal_accounts,
)


tas_test_file = str(settings.APP_DIR / "data" / "testing_data" / "tas_list_1.csv")


@pytest.fixture()
def tas_data():
    call_command("load_tas", location=tas_test_file)


@pytest.mark.django_db(transaction=True)
def test_tas_object(tas_data):
    """Make sure an instance of a tas is properly created."""
    TreasuryAppropriationAccount.objects.get(treasury_account_identifier="7")


@pytest.mark.django_db
def test_federal_account_insert():
    """Test federal account creation from underlying TAS records."""
    baker.make(
        TreasuryAppropriationAccount, agency_id="abc", main_account_code="7777", account_title="Fancy slipper fund"
    )
    baker.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy boot fund",
        beginning_period_of_availability="2015",
        ending_period_of_availability="2016",
    )
    baker.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy flower fund",
        beginning_period_of_availability="2016",
        ending_period_of_availability="2017",
    )
    baker.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy cat fund",
        beginning_period_of_availability=None,
        ending_period_of_availability=None,
    )

    # run the federal account insert process and check results
    insert_federal_accounts()
    link_treasury_accounts_to_federal_accounts()
    federal_accounts = FederalAccount.objects.all()

    # only 1 record per unique agency/main account TAS combo
    assert federal_accounts.count() == 1
    # federal account title should match title of the tas w/ latest BPOA
    # (TAS with a BPOA considered before TAS records with a blank/NULL BPOA)
    fa = federal_accounts[0]
    assert fa.account_title == "Fancy flower fund"
    # federal_account foreign key on TAS records should = id of the federal
    # account we just created
    distinct_fa = TreasuryAppropriationAccount.objects.values("federal_account").distinct()
    assert distinct_fa.count() == 1
    assert distinct_fa[0]["federal_account"] == fa.id


@pytest.mark.django_db
def test_federal_account_update():
    """Test federal account updates from underlying TAS records."""

    fa = baker.make(FederalAccount, id=1, agency_identifier="abc", main_account_code="0987", account_title="Fancy duck")
    tas1 = baker.make(
        TreasuryAppropriationAccount,
        federal_account_id=1,
        agency_id="abc",
        main_account_code="0987",
        account_title="Fancy duck",
    )
    tas2 = baker.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="0987",
        account_title="Fancy goose",
        ending_period_of_availability="2020",
    )

    # run the federal account update process and check results
    update_federal_accounts()
    link_treasury_accounts_to_federal_accounts()

    # federal_account fk of tas2 has been updated
    assert (
        TreasuryAppropriationAccount.objects.get(
            treasury_account_identifier=tas2.treasury_account_identifier
        ).federal_account
        == fa
    )
    # federal_account fk of tas1 is unchanged
    assert (
        TreasuryAppropriationAccount.objects.get(
            treasury_account_identifier=tas1.treasury_account_identifier
        ).federal_account_id
        == 1
    )


@pytest.mark.django_db
def test_remove_empty_federal_accounts():
    """
    Create some federal and treasury accounts.  Ensure that some of the federal accounts are not represented
    in any way, shape, or form by treasury accounts and witness their demise.
    """
    baker.make(FederalAccount, pk=1, agency_identifier="ab1", main_account_code="0987")
    baker.make(FederalAccount, pk=2, agency_identifier="ab2", main_account_code="0987")
    baker.make(FederalAccount, pk=4, agency_identifier="ab4", main_account_code="0987")
    baker.make(FederalAccount, pk=5, agency_identifier="ab5", main_account_code="0987")
    baker.make(FederalAccount, pk=6, agency_identifier="ab6", main_account_code="0987")

    baker.make(TreasuryAppropriationAccount, pk=1, agency_id="ab1", main_account_code="0987", federal_account_id=1)
    baker.make(TreasuryAppropriationAccount, pk=2, agency_id="ab2", main_account_code="0987")

    assert FederalAccount.objects.count() == 5

    remove_empty_federal_accounts()

    assert FederalAccount.objects.count() == 2


@pytest.mark.django_db
def test_link_treasury_accounts_to_federal_accounts():
    """
    Create 3 federal accounts and 5 treasury accounts.  Test the following:
        - properly linked tas is not broken
        - unlinked but linkable tas is linked
        - unlinked unlinkable tas is not linked
        - improperly linked but linkable tas is linked to the correct fa
        - improperly linked unlinkable tas is no longer linked
    """
    baker.make(FederalAccount, pk=1, agency_identifier="ab1", main_account_code="0987")
    baker.make(FederalAccount, pk=2, agency_identifier="ab2", main_account_code="0987")
    baker.make(FederalAccount, pk=4, agency_identifier="ab4", main_account_code="0987")

    baker.make(TreasuryAppropriationAccount, pk=1, agency_id="ab1", main_account_code="0987", federal_account_id=1)
    baker.make(TreasuryAppropriationAccount, pk=2, agency_id="ab2", main_account_code="0987")
    baker.make(TreasuryAppropriationAccount, pk=3, agency_id="ab3", main_account_code="0987")
    baker.make(TreasuryAppropriationAccount, pk=4, agency_id="ab4", main_account_code="0987", federal_account_id=1)
    baker.make(TreasuryAppropriationAccount, pk=5, agency_id="ab5", main_account_code="0987", federal_account_id=1)

    assert TreasuryAppropriationAccount.objects.get(pk=1).federal_account_id == 1
    assert TreasuryAppropriationAccount.objects.get(pk=2).federal_account_id is None
    assert TreasuryAppropriationAccount.objects.get(pk=3).federal_account_id is None
    assert TreasuryAppropriationAccount.objects.get(pk=4).federal_account_id == 1
    assert TreasuryAppropriationAccount.objects.get(pk=5).federal_account_id == 1

    link_treasury_accounts_to_federal_accounts()

    assert TreasuryAppropriationAccount.objects.get(pk=1).federal_account_id == 1
    assert TreasuryAppropriationAccount.objects.get(pk=2).federal_account_id == 2
    assert TreasuryAppropriationAccount.objects.get(pk=3).federal_account_id is None
    assert TreasuryAppropriationAccount.objects.get(pk=4).federal_account_id == 4
    assert TreasuryAppropriationAccount.objects.get(pk=5).federal_account_id is None
