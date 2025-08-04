import pytest
from model_bakery import baker

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.models.dabs_submission_window_schedule import DABSSubmissionWindowSchedule


@pytest.fixture
def basic_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(123)

    _faba_for_award(award)


@pytest.fixture
def award_with_quarterly_submission(award_count_sub_schedule, award_count_quarterly_submission, defc_codes):
    award = _normal_award(345)

    _faba_for_award(award)


@pytest.fixture
def award_with_early_submission(defc_codes):
    award = _normal_award(456)
    _award_count_early_submission()

    _faba_for_award(award)


@pytest.fixture
def file_c_with_no_award(defc_codes):
    _award_count_early_submission()

    _faba_for_award(None)
    _faba_for_award(None, 2)


@pytest.fixture
def multiple_file_c_to_same_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(874)

    _faba_for_award(award)
    _faba_for_award(award)


@pytest.fixture
def multiple_outlay_file_c_to_same_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(923)

    _faba_for_award(award, outlay_based=True)
    _faba_for_award(award, outlay_based=True)


@pytest.fixture
def multiple_file_c_to_same_award_that_cancel_out(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(643)

    _faba_for_award(award)
    _faba_for_award(award, negative=True)


@pytest.fixture
def obligations_incurred_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(592)

    baker.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        parent_award_id="obligations award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        transaction_obligated_amount=8,
    )


@pytest.fixture
def non_matching_defc_award(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award(937)

    baker.make(
        "awards.FinancialAccountsByAwards",
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="A").first(),
        submission=SubmissionAttributes.objects.all().first(),
        transaction_obligated_amount=7,
    )


@pytest.fixture
def award_count_submission():
    baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-04-01",
        submission_window_id=20220800,
    )


def _award_count_early_submission():
    if not DABSSubmissionWindowSchedule.objects.filter(
        submission_fiscal_year=2020
    ):  # hack since in some environments these auto-populate
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id=20200700,
            is_quarter=False,
            submission_fiscal_year=2020,
            submission_fiscal_quarter=3,
            submission_fiscal_month=7,
            submission_reveal_date="2020-5-15",
        )

    baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        reporting_period_start="2020-04-01",
        submission_window_id=20200700,
    )


@pytest.fixture
def award_count_quarterly_submission():
    baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=8,
        quarter_format_flag=True,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-04-01",
        submission_window_id=20220801,
    )


@pytest.fixture
def award_count_sub_schedule():
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=20220800,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=20220801,
        is_quarter=True,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )


def _normal_award(id):
    return baker.make("search.AwardSearch", award_id=id, type="A")


def _faba_for_award(award, id=1, negative=False, outlay_based=False):
    baker.make(
        "awards.FinancialAccountsByAwards",
        piid=f"piid {id}",
        parent_award_id="same parent award",
        fain=f"fain {id}",
        uri=f"uri {id}",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        transaction_obligated_amount=(-7 if negative else 7) if not outlay_based else 0,
        gross_outlay_amount_by_award_cpe=(-7 if negative else 7) if outlay_based else 0,
        distinct_award_key=f"piid {id}|same parent award|fain {id}|uri {id}".upper(),
    )


@pytest.fixture()
def basic_covid_faba_spending_data(defc_codes):
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="awards",
        defc="M",
        award_type="A",
        award_count=1,
        obligation_sum=100,
        outlay_sum=100,
    )
