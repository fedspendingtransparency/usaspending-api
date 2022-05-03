import pytest

from model_bakery import baker

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import Agency


@pytest.fixture
def faba_with_toptier_agencies(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    toptier_agency(2)
    award2 = award_with_toptier_agency(2)
    award3 = baker.make("awards.Award", type="A", funding_agency=Agency.objects.first(), total_loan_value=0)

    faba_for_award(award1, 8, 0)
    faba_for_award(award2, 0, 7)
    faba_for_award(award3, 8, 0)


@pytest.fixture
def faba_with_toptier_agencies_that_cancel_out_in_toa(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    faba_for_award(award1, 8, 0)
    faba_for_award(award1, -5, 0)
    faba_for_award(award1, -3, 0)


@pytest.fixture
def faba_with_toptier_agencies_that_cancel_out_in_outlay(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    faba_for_award(award1, 0, 8)
    faba_for_award(award1, 0, -5)
    faba_for_award(award1, 0, -3)


def faba_for_award(award, toa, outlay):
    return baker.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.filter(reporting_fiscal_year=2022, reporting_fiscal_period=8).first(),
        transaction_obligated_amount=toa,
        gross_outlay_amount_by_award_cpe=outlay,
    )


def toptier_agency(id):
    return baker.make("references.ToptierAgency", pk=id)


def award_with_toptier_agency(id):
    agency = baker.make("references.Agency", toptier_agency_id=id)

    return baker.make("awards.Award", type="A", funding_agency=agency, total_loan_value=0)
