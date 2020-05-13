import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/budgetary_resources/{filter}"
FY = current_fiscal_year()
PRIOR_FY = FY - 1


@pytest.fixture
def data_fixture():
    ta1 = mommy.make("references.ToptierAgency", toptier_code="001")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002")
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    sa1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=FY)
    sa2 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=PRIOR_FY)

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=1,
        final_of_fy=True,
        treasury_account_identifier=tas1,
        submission=sa1,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=3,
        final_of_fy=True,
        treasury_account_identifier=tas1,
        submission=sa1,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=7,
        final_of_fy=False,
        treasury_account_identifier=tas1,
        submission=sa1,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=15,
        final_of_fy=True,
        treasury_account_identifier=tas1,
        submission=sa2,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=31,
        final_of_fy=True,
        treasury_account_identifier=tas2,
        submission=sa1,
    )

    mommy.make(
        "accounts.AppropriationAccountBalances",
        total_budgetary_resources_amount_cpe=63,
        final_of_fy=True,
        treasury_account_identifier=tas2,
        submission=sa2,
    )


@pytest.mark.django_db
def test_budgetary_resources(client, data_fixture):
    resp = client.get(URL.format(code="001", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == FY
    assert resp.data["toptier_code"] == "001"
    assert resp.data["agency_budgetary_resources"] == 4
    assert resp.data["prior_year_agency_budgetary_resources"] == 15
    assert resp.data["total_federal_budgetary_resources"] == 35
    assert resp.data["messages"] == []

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={FY}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == FY
    assert resp.data["toptier_code"] == "001"
    assert resp.data["agency_budgetary_resources"] == 4
    assert resp.data["prior_year_agency_budgetary_resources"] == 15
    assert resp.data["total_federal_budgetary_resources"] == 35
    assert resp.data["messages"] == []

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={PRIOR_FY}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == PRIOR_FY
    assert resp.data["toptier_code"] == "001"
    assert resp.data["agency_budgetary_resources"] == 15
    assert resp.data["prior_year_agency_budgetary_resources"] is None
    assert resp.data["total_federal_budgetary_resources"] == 78
    assert resp.data["messages"] == []
