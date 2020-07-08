import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_fabs_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()
    _normal_faba(award)
    _normal_fabs(award)


@pytest.fixture
def basic_fpds_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    _normal_faba(award)

    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")


@pytest.fixture
def double_fpds_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    _normal_faba(award)

    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")

    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)
    mommy.make("awards.TransactionFPDS", transaction=transaction_normalized, awardee_or_recipient_uniqu="2")


@pytest.fixture
def award_with_no_outlays(award_count_sub_schedule, award_count_submission, defc_codes):
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
        gross_outlays_delivered_orders_paid_total_cpe=0,
    )

    _normal_fabs(award)


@pytest.fixture
def fabs_award_with_quarterly_submission(award_count_sub_schedule, award_count_quarterly_submission, defc_codes):
    award = _normal_award()
    _normal_faba(award)
    _normal_fabs(award)


@pytest.fixture
def fabs_award_with_old_submission(defc_codes, award_count_sub_schedule):
    award = _normal_award()

    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=3,
        submission_reveal_date="2022-5-15",
    )
    old_submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=3,
        quarter_format_flag=False,
        reporting_period_start="2022-04-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=old_submission,
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )
    _normal_fabs(award)


@pytest.fixture
def fabs_award_with_unclosed_submission(defc_codes, award_count_sub_schedule):
    award = _normal_award()

    old_submission = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=11,
        quarter_format_flag=False,
        reporting_period_start="2022-11-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=old_submission,
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )
    _normal_fabs(award)


def _normal_award():
    return mommy.make("awards.Award", type="A")


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
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )


def _normal_fabs(award):
    transaction_normalized = mommy.make("awards.TransactionNormalized", award=award)
    mommy.make("awards.TransactionFABS", transaction=transaction_normalized, awardee_or_recipient_uniqu="1")
