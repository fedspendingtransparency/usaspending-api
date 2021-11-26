import pytest

from model_mommy import mommy

from usaspending_api.common.helpers.fiscal_year_helpers import (
    get_final_period_of_quarter,
    calculate_last_completed_fiscal_quarter,
)

CURRENT_FISCAL_YEAR = 2020


class Helpers:
    @staticmethod
    def get_mocked_current_fiscal_year():
        return CURRENT_FISCAL_YEAR

    @staticmethod
    def mock_current_fiscal_year(monkeypatch, path=None):
        def _mocked_fiscal_year():
            return CURRENT_FISCAL_YEAR

        if path is None:
            path = "usaspending_api.agency.v2.views.agency_base.current_fiscal_year"
        monkeypatch.setattr(path, _mocked_fiscal_year)


@pytest.fixture
def helpers():
    return Helpers


@pytest.fixture
def agency_account_data():
    dabs = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = mommy.make("references.ToptierAgency", toptier_code="007")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="008")
    ta3 = mommy.make("references.ToptierAgency", toptier_code="009")
    ta4 = mommy.make("references.ToptierAgency", toptier_code="010")
    ta5 = mommy.make("references.ToptierAgency", toptier_code="011")

    mommy.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1)
    mommy.make("references.Agency", id=2, toptier_flag=True, toptier_agency=ta2)
    mommy.make("references.Agency", id=3, toptier_flag=True, toptier_agency=ta3)
    mommy.make("references.Agency", id=4, toptier_flag=True, toptier_agency=ta4)
    mommy.make("references.Agency", id=5, toptier_flag=True, toptier_agency=ta5)

    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=12,
        toptier_code=ta2.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2018,
        reporting_fiscal_period=12,
        toptier_code=ta3.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub4 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=12,
        toptier_code=ta4.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub5 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub6 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=8,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub7 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=9,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=12,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    fa1 = mommy.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")
    fa2 = mommy.make("accounts.FederalAccount", federal_account_code="002-0000", account_title="FA 2")
    fa3 = mommy.make("accounts.FederalAccount", federal_account_code="003-0000", account_title="FA 3")
    fa4 = mommy.make("accounts.FederalAccount", federal_account_code="004-0000", account_title="FA 4")
    fa5 = mommy.make("accounts.FederalAccount", federal_account_code="005-0000", account_title="FA 5")
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
    tas7 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta5,
        budget_function_code=700,
        budget_function_title="NAME 7",
        budget_subfunction_code=7000,
        budget_subfunction_title="NAME 7A",
        federal_account=fa5,
        account_title="TA 7",
        tas_rendering_label="005-X-0000-000",
    )

    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3, submission=sub3)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas4, submission=sub4)

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

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    mommy.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10000000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc2,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa3,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub2,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=10000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=1000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100000,
        gross_outlay_amount_by_program_object_class_cpe=100,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=1000000,
        gross_outlay_amount_by_program_object_class_cpe=10,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=10000000,
        gross_outlay_amount_by_program_object_class_cpe=1,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas4,
        submission=sub5,
        program_activity=pa5,
        object_class=oc5,
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas5,
        submission=sub1,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas6,
        submission=sub1,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas7,
        submission=sub6,
        obligations_incurred_by_program_object_class_cpe=700,
        gross_outlay_amount_by_program_object_class_cpe=7000,
    )
    mommy.make(
        fabpaoc,
        treasury_account=tas7,
        submission=sub7,
        obligations_incurred_by_program_object_class_cpe=710,
        gross_outlay_amount_by_program_object_class_cpe=7100,
    )


@pytest.fixture
def bureau_data():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-01-01",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"2018-01-01",
        submission_fiscal_year=2018,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date=f"2018-09-01",
        period_end_date=f"2018-10-01",
    )
    ta1 = mommy.make("references.ToptierAgency", toptier_code="001")
    sa1 = mommy.make("references.SubtierAgency", subtier_code="0001")
    mommy.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, subtier_agency=sa1)
    mommy.make("references.BureauTitleLookup", federal_account_code="001-0000", bureau_slug="test-bureau")
    fa1 = mommy.make(
        "accounts.FederalAccount", account_title="FA 1", federal_account_code="001-0000", parent_toptier_agency=ta1
    )
    taa1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=CURRENT_FISCAL_YEAR,
        fiscal_period=get_final_period_of_quarter(calculate_last_completed_fiscal_quarter(CURRENT_FISCAL_YEAR)) or 12,
        treasury_account_identifier=taa1,
        total_budgetary_resources_cpe=100,
        gross_outlay_amount_by_tas_cpe=10,
        obligations_incurred_total_cpe=1,
    )
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002")
    sa2 = mommy.make("references.SubtierAgency", subtier_code="0002")
    mommy.make("references.Agency", id=2, toptier_flag=True, toptier_agency=ta2, subtier_agency=sa2)
    mommy.make("references.BureauTitleLookup", federal_account_code="002-0000", bureau_slug="test-bureau-2")
    fa1 = mommy.make(
        "accounts.FederalAccount", account_title="FA 1", federal_account_code="002-0000", parent_toptier_agency=ta1
    )
    taa1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)
    mommy.make(
        "references.GTASSF133Balances",
        fiscal_year=2018,
        fiscal_period=12,
        treasury_account_identifier=taa1,
        total_budgetary_resources_cpe=100,
        gross_outlay_amount_by_tas_cpe=10,
        obligations_incurred_total_cpe=1,
    )

__all__ = ["agency_account_data", "helpers", "bureau_data"]
