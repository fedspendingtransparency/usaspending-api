import pytest
import json

from django.core.management import call_command

from model_mommy import mommy

from usaspending_api.awards.models import Award, Transaction, Subaward, TransactionContract, TransactionAssistance
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def test_subaward_etl_fixture():
    test_submission_id = 2727
    test_piid = "DTFAWA11D00051CALL0002"
    test_parent_award_id = "DTFAWA11D00051"
    test_fain = "333SBGP0132011"
    test_uri = None

    submission = mommy.make(SubmissionAttributes, broker_submission_id=test_submission_id)
    prime_award_1 = mommy.make(Award, description="prime_award_1")
    prime_award_2 = mommy.make(Award, description="prime_award_2")

    # Transaction with subcontract
    txn1 = mommy.make(Transaction, award=prime_award_1, submission=submission)
    mommy.make(TransactionContract, transaction=txn1, piid=test_piid, parent_award_id=test_parent_award_id)

    # Transaction with subaward, by FAIN
    txn2 = mommy.make(Transaction, award=prime_award_2, submission=submission)
    mommy.make(TransactionAssistance, transaction=txn2, fain=test_fain, uri=test_uri)


@pytest.mark.django_db
def test_subaward_etl_award_linkages(test_subaward_etl_fixture):
    # First, run the command
    call_command("load_subawards", "-s", "2727", "--test")

    # Make sure we have the right number of subawards
    assert Subaward.objects.count() == 2

    # Check that we have our subcontract
    subcontract = Subaward.objects.filter(subaward_number="33118").first()
    prime_award_1 = Award.objects.filter(description="prime_award_1").first()
    assert subcontract is not None
    assert subcontract.award == prime_award_1
    assert prime_award_1.subaward_count == 1
    assert prime_award_1.total_subaward_amount == subcontract.amount

    # Check out subaward
    subaward2 = Subaward.objects.filter(subaward_number="SBG-02-04-2011").first()
    prime_award_2 = Award.objects.filter(description="prime_award_2").first()
    assert subaward2 is not None
    assert subaward2.award == prime_award_2
    assert prime_award_2.subaward_count == 1
    assert prime_award_2.total_subaward_amount == subaward2.amount
