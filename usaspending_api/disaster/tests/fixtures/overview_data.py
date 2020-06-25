import pytest

from model_mommy import mommy
from usaspending_api.references.models import DisasterEmergencyFundCode


@pytest.fixture
def defc_codes():
    return [mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")]


@pytest.fixture
def basic_gtas(defc_codes):
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2020,
        fiscal_period=12,
        unobligated_balance_cpe=5,
        disaster_emergency_fund_code="M",
        budget_authority_appropriation_amount_cpe=0.8,
        other_budgetary_resources_amount_cpe=0.7,
        gross_outlay_amount_by_tas_cpe=1.9,
    )


@pytest.fixture
def basic_faba(defc_codes):
    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.all().first(),
        transaction_obligated_amount=0.6,
    )
