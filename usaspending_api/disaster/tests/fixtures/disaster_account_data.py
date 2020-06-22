import pytest

from datetime import datetime
from dateutil.relativedelta import relativedelta
from model_mommy import mommy

from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


@pytest.fixture
def disaster_account_data():
    ta1 = mommy.make("references.ToptierAgency", toptier_code="007")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="008")
    ta3 = mommy.make("references.ToptierAgency", toptier_code="009")
    ta4 = mommy.make("references.ToptierAgency", toptier_code="010")
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=current_fiscal_year(),
        reporting_period_start="2020-04-01",
        reporting_period_end="2020-04-20",
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=current_fiscal_year(),
        reporting_period_start="2020-05-01",
        reporting_period_end="2020-04-20",
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=current_fiscal_year(),
        reporting_period_start="2020-06-01",
        reporting_period_end="2020-04-20",
    )
    sub4 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=current_fiscal_year(),
        reporting_period_start="2020-04-01",
        reporting_period_end=datetime.strftime(datetime.now() - relativedelta(years=1), "%Y-%m-%d"),
    )
    sub5 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2019,
        reporting_period_start="2019-04-01",
        reporting_period_end="2019-04-20",
    )
    fa1 = mommy.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")
    fa2 = mommy.make("accounts.FederalAccount", federal_account_code="002-0000", account_title="FA 2")
    fa3 = mommy.make("accounts.FederalAccount", federal_account_code="003-0000", account_title="FA 3")
    fa4 = mommy.make("accounts.FederalAccount", federal_account_code="004-0000", account_title="FA 4")
    tas1 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=100,
        budget_function_title="NAME 1",
        budget_subfunction_code=1100,
        budget_subfunction_title="NAME 1A",
        federal_account=fa1,
        account_title="TA 1",
        tas_rendering_label="001-X-0000-000",
    )
    tas2 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta2,
        budget_function_code=200,
        budget_function_title="NAME 2",
        budget_subfunction_code=2100,
        budget_subfunction_title="NAME 2A",
        federal_account=fa2,
        account_title="TA 2",
        tas_rendering_label="002-X-0000-000",
    )
    tas3 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta3,
        budget_function_code=300,
        budget_function_title="NAME 3",
        budget_subfunction_code=3100,
        budget_subfunction_title="NAME 3A",
        federal_account=fa3,
        account_title="TA 3",
        tas_rendering_label="003-X-0000-000",
    )
    tas4 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta4,
        budget_function_code=400,
        budget_function_title="NAME 4",
        budget_subfunction_code=4100,
        budget_subfunction_title="NAME 4A",
        federal_account=fa4,
        account_title="TA 4",
        tas_rendering_label="001-X-0000-000",
    )
    tas5 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=200,
        budget_function_title="NAME 5",
        budget_subfunction_code=2100,
        budget_subfunction_title="NAME 5A",
        federal_account=fa2,
        account_title="TA 5",
        tas_rendering_label="002-2008/2009-0000-000",
    )
    tas6 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=300,
        budget_function_title="NAME 6",
        budget_subfunction_code=3100,
        budget_subfunction_title="NAME 6A",
        federal_account=fa3,
        account_title="TA 6",
        tas_rendering_label="003-2017/2018-0000-000",
    )

    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas4)

    pa1 = mommy.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = mommy.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")
    pa3 = mommy.make("references.RefProgramActivity", program_activity_code="4567", program_activity_name="NAME 3")
    pa4 = mommy.make("references.RefProgramActivity", program_activity_code="111", program_activity_name="NAME 4")
    pa5 = mommy.make("references.RefProgramActivity", program_activity_code="1234", program_activity_name="NAME 5")

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
    oc5 = mommy.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=140, object_class_name="interest"
    )

    defc = "references.DisasterEmergencyFundCode"
    defc1 = mommy.make(defc, code="L", public_law="PUBLIC LAW FOR CODE L", title="TITLE FOR CODE L")
    defc2 = mommy.make(defc, code="M", public_law="PUBLIC LAW FOR CODE M", title="TITLE FOR CODE M")
    defc3 = mommy.make(defc, code="N", public_law="PUBLIC LAW FOR CODE N", title="TITLE FOR CODE N")
    defc4 = mommy.make(defc, code="O", public_law="PUBLIC LAW FOR CODE O", title="TITLE FOR CODE O")
    defc5 = mommy.make(defc, code="P", public_law="PUBLIC LAW FOR CODE P", title="TITLE FOR CODE P")
    mommy.make(defc, code="9", public_law="PUBLIC LAW FOR CODE 9", title="TITLE FOR CODE 9")

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        disaster_emergency_fund=defc1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10000000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc2,
        disaster_emergency_fund=defc2,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa3,
        object_class=oc3,
        disaster_emergency_fund=defc5,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas2,
        submission=sub2,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc4,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=10000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas2,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        disaster_emergency_fund=defc3,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=1000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas3,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        disaster_emergency_fund=defc3,
        obligations_incurred_by_program_object_class_cpe=100000,
        gross_outlay_amount_by_program_object_class_cpe=100,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc4,
        obligations_incurred_by_program_object_class_cpe=1000000,
        gross_outlay_amount_by_program_object_class_cpe=10,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc4,
        obligations_incurred_by_program_object_class_cpe=10000000,
        gross_outlay_amount_by_program_object_class_cpe=1,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas4,
        submission=sub5,
        program_activity=pa5,
        object_class=oc5,
        disaster_emergency_fund=defc5,
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas5,
        submission=sub1,
        disaster_emergency_fund=None,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas6,
        submission=sub1,
        disaster_emergency_fund=None,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
    )
