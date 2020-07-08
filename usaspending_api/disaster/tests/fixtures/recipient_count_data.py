import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_fabs_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )

    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)

    mommy.make("awards.TransactionFABS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")


@pytest.fixture
def basic_fpds_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )

    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)

    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")


def _normal_award():
    return mommy.make("awards.Award", type="A")
