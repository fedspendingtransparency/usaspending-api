import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.disaster.v2.views.disaster_base import COVID_19_GROUP_NAME


@pytest.fixture
def basic_award(defc_codes):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022081",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2022-5-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022080",
        is_quarter=True,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2022-5-15",
    )

    award = mommy.make("awards.Award")

    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        reporting_period_start="2022-04-01",
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME).first(),
        submission=sub1,
        gross_outlay_amount_by_award_cpe=8,
    )
