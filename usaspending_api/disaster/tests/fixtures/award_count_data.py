import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.models.dabs_submission_window_schedule import DABSSubmissionWindowSchedule


@pytest.fixture
def basic_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )


@pytest.fixture
def award_with_quarterly_submission(award_count_sub_schedule, award_count_quarterly_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )


@pytest.fixture
def award_with_early_submission(defc_codes):
    award = _normal_award()
    _award_count_early_submission()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlays_delivered_orders_paid_total_cpe=8,
    )


@pytest.fixture
def obligations_incurred_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        obligations_incurred_total_by_award_cpe=8,
    )


@pytest.fixture
def non_matching_defc_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="A").first(),
        submission=SubmissionAttributes.objects.all().first(),
        obligations_incurred_total_by_award_cpe=8,
    )


@pytest.fixture
def not_last_submission_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()

    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=10,
        submission_reveal_date="2022-5-15",
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="A").first(),
        submission=SubmissionAttributes.objects.all().first(),
        obligations_incurred_total_by_award_cpe=8,
    )


@pytest.fixture
def award_count_submission():
    mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        reporting_period_start="2022-04-01",
    )


def _award_count_early_submission():
    if not DABSSubmissionWindowSchedule.objects.filter(
        submission_fiscal_year=2020
    ):  # hack since in some environments these auto-populate
        mommy.make(
            "submissions.DABSSubmissionWindowSchedule",
            is_quarter=False,
            submission_fiscal_year=2020,
            submission_fiscal_quarter=3,
            submission_fiscal_month=7,
            submission_reveal_date="2020-5-15",
        )

    mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        reporting_period_start="2020-04-01",
    )


@pytest.fixture
def award_count_quarterly_submission():
    mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=8,
        quarter_format_flag=True,
        reporting_period_start="2022-04-01",
    )


@pytest.fixture
def award_count_sub_schedule():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2022-5-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=True,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2022-5-15",
    )


def _normal_award():
    return mommy.make("awards.Award", type="A")
