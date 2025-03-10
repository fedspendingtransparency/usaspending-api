import pytest

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction
from model_bakery import baker
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.awards.models import Award, TransactionFPDS, TransactionNormalized
from usaspending_api.references.management.commands.load_agencies import Command, Agency as AgencyTuple
from usaspending_api.references.models import Agency, SubtierAgency, ToptierAgency
from usaspending_api.references.models import CGAC, FREC
from usaspending_api.search.models import SubawardSearch, TransactionSearch
from usaspending_api import settings


AGENCY_FILE = settings.APP_DIR / "references" / "tests" / "data" / "test_load_agencies.csv"
BOGUS_ABBREVIATION = "THIS IS A TEST ABBREVIATION"


@pytest.fixture
def disable_vacuuming(monkeypatch):
    """
    We cannot run vacuums in a transaction.  Since tests are run in a transaction, we'll NOOP the
    function that performs the vacuuming.
    """
    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._vacuum_tables", lambda a: None
    )


def _get_record_count():
    return (
        ToptierAgency.objects.count()
        + SubtierAgency.objects.count()
        + Agency.objects.count()
        + CGAC.objects.count()
        + FREC.objects.count()
    )


@pytest.mark.django_db(transaction=True)
def test_happy_path():
    """
    We're running this one without a transaction just to ensure the vacuuming doesn't blow up.  For
    the remaining tests we'll run inside of a transaction since it's faster.
    """

    # Confirm everything is empty.
    assert CGAC.objects.count() == 0
    assert FREC.objects.count() == 0
    assert SubtierAgency.objects.count() == 0
    assert ToptierAgency.objects.count() == 0
    assert Agency.objects.count() == 0

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Confirm nothing is empty.
    assert CGAC.objects.count() > 0
    assert FREC.objects.count() > 0
    assert SubtierAgency.objects.count() > 0
    assert ToptierAgency.objects.count() > 0
    assert Agency.objects.count() > 0


@pytest.mark.django_db
def test_no_file_provided():

    # This should error since agency file is required.
    with pytest.raises(CommandError):
        call_command("load_agencies")


@pytest.mark.django_db
def test_create_agency(disable_vacuuming, monkeypatch):
    """Let's add an agency record to the "raw" file and see what happens."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    record_count = _get_record_count()

    original_read_raw_agencies_csv = Command._read_raw_agencies_csv

    def add_agency(self):
        original_read_raw_agencies_csv(self)
        self.agencies.append(
            AgencyTuple(
                row_number=len(self.agencies) + 1,
                cgac_agency_code="123",
                agency_name="BOGUS CGAC NAME",
                agency_abbreviation="BOGUS CGAC ABBREVIATION",
                frec="4567",
                frec_entity_description="BOGUS FREC NAME",
                frec_abbreviation="BOGUS FREC ABBREVIATION",
                subtier_code="8901",
                subtier_name="BOGUS SUBTIER NAME",
                subtier_abbreviation="BOGUS SUBTIER ABBREVIATION",
                toptier_flag=True,
                is_frec=False,
                frec_cgac_association=False,
                user_selectable=True,
                mission="BOGUS MISSION",
                about_agency_data="BOGUS ABOUT AGENCY DATA",
                website="BOGUS WEBSITE",
                congressional_justification="BOGUS CONGRESSIONAL JUSTIFICATION",
                icon_filename="BOGUS ICON FILENAME",
            )
        )

    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._read_raw_agencies_csv", add_agency
    )

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # 1 toptier + 1 subtier + 1 agency + 1 CGAC + 1 FREC = 5 things
    assert _get_record_count() == record_count + 5


@pytest.mark.django_db
def test_update_agency(disable_vacuuming):
    """Also confirm agency data is updated in place instead of being recreated as a new record."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    record_count = _get_record_count()

    # Grab a toptier, a subtier, and an agency.
    toptier_agency = ToptierAgency.objects.first()
    subtier_agency = SubtierAgency.objects.first()
    agency = Agency.objects.first()

    # Make a change to each.
    ToptierAgency.objects.filter(pk=toptier_agency.pk).update(abbreviation=BOGUS_ABBREVIATION)
    SubtierAgency.objects.filter(pk=subtier_agency.pk).update(abbreviation=BOGUS_ABBREVIATION)
    Agency.objects.filter(pk=agency.pk).update(toptier_flag=(not agency.toptier_flag))

    # Confirm our changes took.  (Yes, we're testing our tests.)
    assert ToptierAgency.objects.get(pk=toptier_agency.pk).abbreviation == BOGUS_ABBREVIATION
    assert SubtierAgency.objects.get(pk=subtier_agency.pk).abbreviation == BOGUS_ABBREVIATION
    assert Agency.objects.get(pk=agency.pk).toptier_flag == (not agency.toptier_flag)

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Confirm our changes were reverted.  Coincidentally, this also confirms that our ids didn't change
    # which means our records were updated in place instead of being recreated since "get" would blow up
    # if the ids were different.
    assert ToptierAgency.objects.get(pk=toptier_agency.pk).abbreviation == toptier_agency.abbreviation
    assert SubtierAgency.objects.get(pk=subtier_agency.pk).abbreviation == subtier_agency.abbreviation
    assert Agency.objects.get(pk=agency.pk).toptier_flag == agency.toptier_flag

    # Ensure nothing new was added.
    assert record_count == _get_record_count()


@pytest.mark.django_db
def test_delete_agency(disable_vacuuming, monkeypatch):
    """Let's remove an entire toptier agency and see what happens."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    toptier_count = ToptierAgency.objects.count()
    subtier_count = SubtierAgency.objects.count()
    agency_count = Agency.objects.count()
    cgac_count = CGAC.objects.count()
    frec_count = FREC.objects.count()

    original_read_raw_agencies_csv = Command._read_raw_agencies_csv

    def remove_toptier_agency(self):
        original_read_raw_agencies_csv(self)
        toptier_code = self.agencies[0].cgac_agency_code
        self.agencies = [a for a in self.agencies if a.cgac_agency_code != toptier_code]

    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._read_raw_agencies_csv",
        remove_toptier_agency,
    )

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Make sure the data was affected as expected.
    assert ToptierAgency.objects.count() == toptier_count - 1
    assert SubtierAgency.objects.count() == subtier_count - 3
    assert Agency.objects.count() == agency_count - 3
    assert CGAC.objects.count() == cgac_count - 1
    assert FREC.objects.count() == frec_count  # This frec is associated with more than one agency in our test data


@pytest.mark.django_db
def test_update_treasury_appropriation_account(disable_vacuuming):

    # Create a bogus TAS.
    baker.make("accounts.TreasuryAppropriationAccount", agency_id="009")

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Ensure our TAS got updated.
    assert (
        TreasuryAppropriationAccount.objects.first().funding_toptier_agency_id
        == ToptierAgency.objects.get(toptier_code="009").toptier_agency_id
    )

    # Set it to something else and reload to make sure it gets updated.
    TreasuryAppropriationAccount.objects.update(agency_id="005")

    # Double check.
    assert TreasuryAppropriationAccount.objects.first().agency_id == "005"

    # So there's a cutoff in the code to skip "expensive" steps if the agency data didn't
    # change.  Let's make a small tweak to the agency data to ensure TAS gets updated.
    ToptierAgency.objects.update(abbreviation=BOGUS_ABBREVIATION)

    # Reload the agency file.
    call_command("load_agencies", AGENCY_FILE)

    # Was it fixed?
    assert (
        TreasuryAppropriationAccount.objects.first().funding_toptier_agency_id
        == ToptierAgency.objects.get(toptier_code="005").toptier_agency_id
    )


@pytest.mark.django_db
def test_update_transactions_awards_subawards(disable_vacuuming):
    """Test all three together since they're so tightly intertwined."""

    # Create some test data.
    a = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="AWARD_1")
    tn = baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        is_fpds=True,
        award=a,
        generated_unique_award_id="AWARD_1",
        funding_agency_id="0901",
        funding_sub_tier_agency_co="0901",
    )
    a.latest_transaction_id = tn.transaction_id
    a.save()
    baker.make("search.SubawardSearch", award=a, unique_award_key="AWARD_1")

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Ensure our transaction and award got updated.
    agency_id = Agency.objects.get(subtier_agency__subtier_code="0901").id
    assert TransactionNormalized.objects.first().funding_agency_id == agency_id
    assert Award.objects.first().funding_agency_id == agency_id
    assert SubawardSearch.objects.first().funding_agency_id == agency_id

    # Set it to something else and reload to make sure it gets updated.
    TransactionSearch.objects.update(funding_sub_tier_agency_co="0501")

    # Double check.
    assert TransactionFPDS.objects.first().funding_sub_tier_agency_co == "0501"

    # So there's a cutoff in the code to skip "expensive" steps if the agency data didn't
    # change.  Let's make a small tweak to the agency data to ensure transactions and awards
    # get updated.
    ToptierAgency.objects.update(abbreviation=BOGUS_ABBREVIATION)

    # Reload the agency file.
    call_command("load_agencies", AGENCY_FILE)

    # Was it fixed?
    agency_id = Agency.objects.get(subtier_agency__subtier_code="0501").id
    assert TransactionNormalized.objects.first().funding_agency_id == agency_id
    assert Award.objects.first().funding_agency_id == agency_id
    assert SubawardSearch.objects.first().funding_agency_id == agency_id


@pytest.mark.django_db
def test_exceeding_max_changes(disable_vacuuming, monkeypatch):
    """
    We have a safety cutoff to prevent accidentally updating every record in the database.  Make
    sure we get an exception if we exceed that threshold.
    """
    monkeypatch.setattr("usaspending_api.references.management.commands.load_agencies.MAX_CHANGES", 1)

    with transaction.atomic():  # make it part of this tests transaction
        with pytest.raises(RuntimeError):
            call_command("load_agencies", AGENCY_FILE)

    # Confirm everything is still empty.
    assert CGAC.objects.count() == 0
    assert FREC.objects.count() == 0
    assert SubtierAgency.objects.count() == 0
    assert ToptierAgency.objects.count() == 0
    assert Agency.objects.count() == 0

    with transaction.atomic():
        # Running with force flag should succeed.
        call_command("load_agencies", "--force", AGENCY_FILE)

    # Confirm nothing is empty.
    assert CGAC.objects.count() > 0
    assert FREC.objects.count() > 0
    assert SubtierAgency.objects.count() > 0
    assert ToptierAgency.objects.count() > 0
    assert Agency.objects.count() > 0
