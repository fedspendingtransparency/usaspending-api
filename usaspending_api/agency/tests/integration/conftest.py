import pytest

from model_mommy import mommy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


@pytest.fixture
def agency_account_data():
    ta1 = mommy.make("references.ToptierAgency", toptier_code="007")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="008")
    ta3 = mommy.make("references.ToptierAgency", toptier_code="009")
    sub1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=current_fiscal_year())
    sub2 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    sub3 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2018)
    sub4 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2019)
    fa1 = mommy.make("accounts.FederalAccount")
    fa2 = mommy.make("accounts.FederalAccount")
    fa3 = mommy.make("accounts.FederalAccount")
    tas1 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=100,
        budget_subfunction_code=1100,
        federal_account=fa1
    )
    tas2 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta2,
        budget_function_code=200,
        budget_subfunction_code=2100,
        federal_account=fa2
    )
    tas3 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta3,
        budget_function_code=300,
        budget_subfunction_code=3100,
        federal_account=fa3
    )

    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3)
    pa1 = mommy.make("references.RefProgramActivity", program_activity_code="000")
    pa2 = mommy.make("references.RefProgramActivity", program_activity_code="1000")
    pa3 = mommy.make("references.RefProgramActivity", program_activity_code="4567")
    pa4 = mommy.make("references.RefProgramActivity", program_activity_code="111")

    oc = "references.ObjectClass"
    oc1 = mommy.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=100, object_class_name="equipment"
    )
    oc2 = mommy.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=110, object_class_name="hvac"
    )
    oc3 = mommy.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=120, object_class_name="supplies"
    )
    oc4 = mommy.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=130, object_class_name="interest"
    )

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa1, object_class=oc1
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa2, object_class=oc2
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa3, object_class=oc3
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas2, submission=sub2, program_activity=pa4, object_class=oc4
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas2, submission=sub3, program_activity=pa4, object_class=oc3
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub3, program_activity=pa4, object_class=oc3
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub4, program_activity=pa4, object_class=oc4
    )
    mommy.make(
        fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub4, program_activity=pa4, object_class=oc4
    )


__all__ = ["agency_account_data"]
