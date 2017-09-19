import pytest
import json

from django.core.management import call_command

from model_mommy import mommy

from usaspending_api.awards.models import Award, Subaward
from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def test_subaward_etl_fixture():
    test_submission_id = 2727
    test_piid = "DTFAWA11D00051CALL0002"
    test_parent_award_id = "DTFAWA11D00051"
    test_fain = "333SBGP0132011"
    test_uri = "abcd"
    test_awarding_subtier = "6920"
    test_awarding_toptier = "069"

    submission = mommy.make(SubmissionAttributes, broker_submission_id=test_submission_id)
    tt_agency = mommy.make('references.ToptierAgency', cgac_code=test_awarding_toptier)
    st_agency = mommy.make('references.SubtierAgency', subtier_code=test_awarding_subtier)
    agency = mommy.make('references.Agency', toptier_agency=tt_agency, subtier_agency=st_agency)
    prime_award_1 = mommy.make(Award, description="prime_award_1", awarding_agency=agency)
    prime_award_2 = mommy.make(Award, description="prime_award_2", awarding_agency=agency)
    prime_award_3 = mommy.make(Award, description="prime_award_3")  # NO AGENCY HERE!

    # TransactionNormalized with subcontract
    txn1 = mommy.make(TransactionNormalized, award=prime_award_1)
    mommy.make(TransactionFPDS, transaction=txn1, piid=test_piid, parent_award_id=test_parent_award_id)

    # TransactionNormalized with subaward, by FAIN
    txn2 = mommy.make(TransactionNormalized, award=prime_award_2)
    txn3 = mommy.make(TransactionNormalized, award=prime_award_2)
    mommy.make(TransactionFABS, transaction=txn2, fain=test_fain, uri=None)
    mommy.make(TransactionFABS, transaction=txn3, fain=None, uri=test_uri)

    # Give our agency-less award a transaction that should map to a subaward, to test that agency restricts
    txn4 = mommy.make(TransactionNormalized, award=prime_award_3)
    mommy.make(TransactionFPDS, transaction=txn1, piid=test_piid, parent_award_id=test_parent_award_id)


@pytest.mark.django_db
def test_subaward_etl_award_linkages(test_subaward_etl_fixture):
    # First, run the command
    call_command("load_subawards", "-s", "2727", "--test")

    # Make sure we have the right number of subawards
    assert Subaward.objects.count() == 3

    # Check that we have our subcontract
    subcontract = Subaward.objects.filter(subaward_number="33118").first()
    prime_award_1 = Award.objects.filter(description="prime_award_1").first()
    assert subcontract is not None
    assert subcontract.award == prime_award_1
    assert prime_award_1.subaward_count == 1
    assert prime_award_1.total_subaward_amount == subcontract.amount

    # Check out subaward
    # This one matches on FAIN
    subaward1 = Subaward.objects.filter(subaward_number="SBG-02-04-2011").first()
    # This one matches on URI
    subaward2 = Subaward.objects.filter(subaward_number="SBG-02-04-2011-2").first()
    prime_award_2 = Award.objects.filter(description="prime_award_2").first()
    assert subaward1 is not None
    assert subaward2 is not None
    assert subaward1.award == prime_award_2
    assert subaward2.award == prime_award_2
    assert prime_award_2.subaward_count == 2
    assert prime_award_2.total_subaward_amount == (subaward1.amount + subaward2.amount)

    # Make sure our award w/o agency but w/ matching id's didn't get an award
    prime_award_3 = Award.objects.filter(description="prime_award_3").first()
    assert prime_award_3.subaward_count == 0
