import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def faba_with_toptier_agencies(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    toptier_agency(2)
    award2 = award_with_toptier_agency(2)

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award1,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award2,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )


def toptier_agency(id):
    return mommy.make("references.ToptierAgency", pk=id)


def award_with_toptier_agency(id):
    agency = mommy.make("references.Agency", toptier_agency_id=id)

    return mommy.make("awards.Award", type="A", funding_agency=agency)
