import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_fabs_award(award_count_sub_schedule, award_count_submission, defc_codes):
    _normal_faba(_normal_fabs(1))


@pytest.fixture
def basic_fpds_award(award_count_sub_schedule, award_count_submission, defc_codes):

    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=100, action_date="2022-05-01", is_fpds=True
    )
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="fpds")
    _normal_faba(mommy.make("awards.Award", id=100, latest_transaction=transaction_normalized, type="A"))


@pytest.fixture
def double_fpds_awards_with_distict_recipients(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=200, action_date="2022-05-01", is_fpds=True
    )
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")
    _normal_faba(mommy.make("awards.Award", id=200, latest_transaction=transaction_normalized, type="A"))

    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=300, action_date="2022-05-01", is_fpds=True
    )
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="2")
    _normal_faba(mommy.make("awards.Award", id=300, latest_transaction=transaction_normalized, type="A"))


@pytest.fixture
def double_fpds_awards_with_same_recipients(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=400, action_date="2022-05-01", is_fpds=True
    )
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")
    _normal_faba(mommy.make("awards.Award", id=400, latest_transaction=transaction_normalized, type="A"))

    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=500, action_date="2022-05-01", is_fpds=True
    )
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")
    _normal_faba(mommy.make("awards.Award", id=500, latest_transaction=transaction_normalized, type="A"))


@pytest.fixture
def double_fpds_awards_with_same_special_case_recipients(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=600, action_date="2022-05-01", is_fpds=True
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=transaction_normalized,
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu="123",
    )
    _normal_faba(mommy.make("awards.Award", id=600, latest_transaction=transaction_normalized, type="A"))

    transaction_normalized = mommy.make(
        "awards.TransactionNormalized", award_id=700, action_date="2022-05-01", is_fpds=True
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=transaction_normalized,
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu="456",
    )
    _normal_faba(mommy.make("awards.Award", id=700, latest_transaction=transaction_normalized, type="A"))


@pytest.fixture
def award_with_no_outlays(award_count_sub_schedule, award_count_submission, defc_codes):
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlay_amount_by_award_cpe=0,
    )


@pytest.fixture
def fabs_award_with_quarterly_submission(award_count_sub_schedule, award_count_quarterly_submission, defc_codes):
    _normal_faba(_normal_fabs(1))


@pytest.fixture
def fabs_award_with_old_submission(defc_codes, award_count_sub_schedule):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=20220300,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=3,
        submission_reveal_date="2020-5-15",
    )
    old_submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=3,
        quarter_format_flag=False,
        reporting_period_start="2022-04-01",
        submission_window_id=20220300,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=old_submission,
        transaction_obligated_amount=8,
    )


@pytest.fixture
def fabs_award_with_unclosed_submission(defc_codes, award_count_sub_schedule):
    old_submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=11,
        quarter_format_flag=False,
        reporting_period_start="2022-11-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=old_submission,
        gross_outlay_amount_by_award_cpe=8,
    )


def _normal_faba(award):
    return mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlay_amount_by_award_cpe=8,
    )


def _normal_fabs(id):
    award = mommy.make("awards.Award", latest_transaction_id=id, type="07")
    transaction_normalized = mommy.make("awards.TransactionNormalized", pk=id, award=award, action_date="2022-05-01")
    mommy.make("awards.TransactionFABS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")
    return award
