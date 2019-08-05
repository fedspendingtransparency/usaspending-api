import os

from django.core.management import call_command
from django.conf import settings
from model_mommy import mommy
import pytest

from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.references.reference_helpers import insert_federal_accounts, update_federal_accounts

tas_test_file = os.path.join(settings.BASE_DIR, "usaspending_api/data/testing_data/tas_list_1.csv")


@pytest.fixture()
def tas_data():
    call_command("loadtas", tas_test_file)


@pytest.mark.django_db(transaction=True)
def test_tas_object(tas_data):
    """Make sure an instance of a tas is properly created."""
    TreasuryAppropriationAccount.objects.get(treasury_account_identifier="7")


@pytest.mark.django_db
def test_federal_account_insert():
    """Test federal account creation from underlying TAS records."""
    mommy.make(
        TreasuryAppropriationAccount, agency_id="abc", main_account_code="7777", account_title="Fancy slipper fund"
    )
    mommy.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy boot fund",
        ending_period_of_availability="2016",
    )
    mommy.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy flower fund",
        ending_period_of_availability="2017",
    )
    mommy.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="7777",
        account_title="Fancy cat fund",
        ending_period_of_availability="",
    )

    # run the federal account insert process and check results
    insert_federal_accounts()
    federal_accounts = FederalAccount.objects.all()

    # only 1 record per unique agency/main account TAS combo
    assert federal_accounts.count() == 1
    # federal account title should match title of the tas w/ latest EPOA
    # (TAS with an EPOA considered before TAS records with a blank/NULL EPOA)
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

    fa = mommy.make(FederalAccount, id=1, agency_identifier="abc", main_account_code="0987", account_title="Fancy duck")
    tas1 = mommy.make(
        TreasuryAppropriationAccount,
        federal_account_id=1,
        agency_id="abc",
        main_account_code="0987",
        account_title="Fancy duck",
    )
    tas2 = mommy.make(
        TreasuryAppropriationAccount,
        agency_id="abc",
        main_account_code="0987",
        account_title="Fancy goose",
        ending_period_of_availability="2020",
    )

    # run the federal account update process and check results
    update_federal_accounts()

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
def test_federal_account_update_subset():
    """Test ability to update federal_account FK on a subset of TAS records."""

    fa = mommy.make(FederalAccount, id=1, agency_identifier="abc", main_account_code="0987", account_title="Fancy duck")
    mommy.make(TreasuryAppropriationAccount, agency_id="abc", main_account_code="0987", _quantity=4)

    # send two TAS records to the federal account update process and check results
    update_federal_accounts(
        (
            TreasuryAppropriationAccount.objects.first().treasury_account_identifier,
            TreasuryAppropriationAccount.objects.last().treasury_account_identifier,
        )
    )

    # only two of the four TAS records were updated with a foreign key, even
    # though all four map back to the same federal account
    assert TreasuryAppropriationAccount.objects.filter(federal_account__isnull=True).count() == 2
    # the other two records have federal account FKs
    assert TreasuryAppropriationAccount.objects.filter(federal_account=fa).count() == 2
