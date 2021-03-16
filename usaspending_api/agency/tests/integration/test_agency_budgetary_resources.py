import pytest

from decimal import Decimal
from model_mommy import mommy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/budgetary_resources/{filter}"
FY = current_fiscal_year() - 2
PRIOR_FY = FY - 1


@pytest.fixture
def data_fixture():
    ta1 = mommy.make("references.ToptierAgency", toptier_code="001")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002")
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    sa1_3 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=FY, reporting_fiscal_period=3)
    sa1_6 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=FY, reporting_fiscal_period=6)
    sa1_9 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=FY, reporting_fiscal_period=9)
    sa1_12 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=FY, reporting_fiscal_period=12)
    sa2_12 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=PRIOR_FY, reporting_fiscal_period=12)

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9993,
        obligations_incurred_total_by_tas_cpe=8883,
        treasury_account_identifier=tas1,
        submission=sa1_3,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9996,
        obligations_incurred_total_by_tas_cpe=8886,
        treasury_account_identifier=tas1,
        submission=sa1_6,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=9999,
        obligations_incurred_total_by_tas_cpe=8889,
        treasury_account_identifier=tas1,
        submission=sa1_9,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=1,
        obligations_incurred_total_by_tas_cpe=1,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=3,
        obligations_incurred_total_by_tas_cpe=2,
        treasury_account_identifier=tas1,
        submission=sa1_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=15,
        obligations_incurred_total_by_tas_cpe=5,
        treasury_account_identifier=tas1,
        submission=sa2_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=31,
        obligations_incurred_total_by_tas_cpe=6,
        treasury_account_identifier=tas2,
        submission=sa1_12,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=63,
        obligations_incurred_total_by_tas_cpe=7,
        treasury_account_identifier=tas2,
        submission=sa2_12,
    )


@pytest.mark.django_db
def test_budgetary_resources(client, data_fixture):
    resp = client.get(URL.format(code="001", filter=""))
    assert resp.data == {
        "toptier_code": "001",
        "agency_data_by_year": [
            {
                "fiscal_year": FY,
                "agency_budgetary_resources": Decimal("29992.00"),
                "federal_budgetary_resources": Decimal("30023.00"),
                "agency_total_obligated": Decimal("26661.00"),
            },
            {
                "fiscal_year": PRIOR_FY,
                "agency_budgetary_resources": Decimal("15.00"),
                "federal_budgetary_resources": Decimal("78.00"),
                "agency_total_obligated": Decimal("5.00"),
            },
        ],
        "messages": [],
    }
