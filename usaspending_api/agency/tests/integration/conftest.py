import pytest

from model_bakery import baker

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
def bureau_data():

    ta1 = baker.make("references.ToptierAgency", name="Agency 1", toptier_code="001", _fill_optional=True)
    sa1 = baker.make("references.SubtierAgency", name="Agency 1", subtier_code="0001", _fill_optional=True)

    ta2 = baker.make("references.ToptierAgency", name="Agency 2", toptier_code="002", _fill_optional=True)
    sa2 = baker.make("references.SubtierAgency", name="Agency 2", subtier_code="0002", _fill_optional=True)

    dabs1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-01-01",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )
    dabs2 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2018-01-01",
        submission_fiscal_year=2018,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date="2018-09-01",
        period_end_date="2018-10-01",
    )
    sub_2020_ta1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs1.id,
    )
    sub_2018_ta2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2018,
        reporting_fiscal_period=12,
        toptier_code=ta2.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs2.id,
    )
    sub_2018_ta1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2018,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs2.id,
    )

    baker.make(
        "references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, subtier_agency=sa1, _fill_optional=True
    )
    baker.make(
        "references.BureauTitleLookup",
        federal_account_code="001-0000",
        bureau_title="Test Bureau 1",
        bureau_slug="test-bureau-1",
    )
    fa1 = baker.make(
        "accounts.FederalAccount", account_title="FA 1", federal_account_code="001-0000", parent_toptier_agency=ta1
    )
    taa1 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)

    baker.make(
        "references.Agency", id=2, toptier_flag=True, toptier_agency=ta2, subtier_agency=sa2, _fill_optional=True
    )
    baker.make(
        "references.BureauTitleLookup",
        federal_account_code="002-0000",
        bureau_title="Test Bureau 2",
        bureau_slug="test-bureau-2",
    )
    fa2 = baker.make(
        "accounts.FederalAccount", account_title="FA 2", federal_account_code="002-0000", parent_toptier_agency=ta2
    )
    taa2 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa2)

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=taa1,
        submission=sub_2020_ta1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=taa1,
        submission=sub_2020_ta1,
        total_budgetary_resources_amount_cpe=100,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=taa2,
        submission=sub_2018_ta2,
        obligations_incurred_by_program_object_class_cpe=20,
        gross_outlay_amount_by_program_object_class_cpe=200,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=taa2,
        submission=sub_2018_ta2,
        total_budgetary_resources_amount_cpe=2000,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=taa1,
        submission=sub_2018_ta1,
        obligations_incurred_by_program_object_class_cpe=20,
        gross_outlay_amount_by_program_object_class_cpe=200,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=taa1,
        submission=sub_2018_ta1,
        total_budgetary_resources_amount_cpe=2000,
    )


@pytest.fixture
def agency_account_data():
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = baker.make("references.ToptierAgency", toptier_code="007", _fill_optional=True)
    ta2 = baker.make("references.ToptierAgency", toptier_code="008", _fill_optional=True)
    ta3 = baker.make("references.ToptierAgency", toptier_code="009", _fill_optional=True)
    ta4 = baker.make("references.ToptierAgency", toptier_code="010", _fill_optional=True)
    ta5 = baker.make("references.ToptierAgency", toptier_code="011", _fill_optional=True)

    baker.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, _fill_optional=True)
    baker.make("references.Agency", id=2, toptier_flag=True, toptier_agency=ta2, _fill_optional=True)
    baker.make("references.Agency", id=3, toptier_flag=True, toptier_agency=ta3, _fill_optional=True)
    baker.make("references.Agency", id=4, toptier_flag=True, toptier_agency=ta4, _fill_optional=True)
    baker.make("references.Agency", id=5, toptier_flag=True, toptier_agency=ta5, _fill_optional=True)

    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=12,
        toptier_code=ta2.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2018,
        reporting_fiscal_period=12,
        toptier_code=ta3.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub4 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=12,
        toptier_code=ta4.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub5 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub6 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=8,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub7 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=9,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=12,
        toptier_code=ta5.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    fa1 = baker.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")
    fa2 = baker.make("accounts.FederalAccount", federal_account_code="002-0000", account_title="FA 2")
    fa3 = baker.make("accounts.FederalAccount", federal_account_code="003-0000", account_title="FA 3")
    fa4 = baker.make("accounts.FederalAccount", federal_account_code="004-0000", account_title="FA 4")
    fa5 = baker.make("accounts.FederalAccount", federal_account_code="005-0000", account_title="FA 5")
    tas1 = baker.make(
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
    tas2 = baker.make(
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
    tas3 = baker.make(
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
    tas4 = baker.make(
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
    tas5 = baker.make(
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
    tas6 = baker.make(
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
    tas7 = baker.make(
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

    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub2)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3, submission=sub3)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas4, submission=sub4)

    pa1 = baker.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = baker.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")
    pa3 = baker.make("references.RefProgramActivity", program_activity_code="4567", program_activity_name="NAME 3")
    pa4 = baker.make("references.RefProgramActivity", program_activity_code="111", program_activity_name="NAME 4")
    pa5 = baker.make("references.RefProgramActivity", program_activity_code="1234", program_activity_name="NAME 5")

    oc = "references.ObjectClass"
    oc1 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=100, object_class_name="equipment"
    )
    oc2 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=110, object_class_name="hvac"
    )
    oc3 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=120, object_class_name="supplies"
    )
    oc4 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=130, object_class_name="interest"
    )
    oc5 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=140, object_class_name="interest"
    )

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10000000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc2,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa3,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub2,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=10000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub2,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=10000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="P",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=1000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100000,
        gross_outlay_amount_by_program_object_class_cpe=100,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=1000000,
        gross_outlay_amount_by_program_object_class_cpe=10,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        obligations_incurred_by_program_object_class_cpe=10000000,
        gross_outlay_amount_by_program_object_class_cpe=1,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas4,
        submission=sub5,
        program_activity=pa5,
        object_class=oc5,
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas5,
        program_activity=pa5,
        object_class=oc5,
        submission=sub1,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas6,
        submission=sub1,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas7,
        submission=sub6,
        obligations_incurred_by_program_object_class_cpe=700,
        gross_outlay_amount_by_program_object_class_cpe=7000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas7,
        submission=sub7,
        obligations_incurred_by_program_object_class_cpe=710,
        gross_outlay_amount_by_program_object_class_cpe=7100,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )


@pytest.fixture
def tas_mulitple_pas_per_oc():
    """Sets up test data such that for a specific treasury account there are multiple
    program activites associated with that treasury account and one object class.
    """
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = baker.make("references.ToptierAgency", toptier_code="007", _fill_optional=True)
    fa1 = baker.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")

    pa1 = baker.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = baker.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")

    oc = "references.ObjectClass"
    oc1 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=100, object_class_name="equipment"
    )

    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )

    tas1 = baker.make(
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

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"

    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10000000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )

    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )


@pytest.fixture
def tas_with_no_object_class():
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = baker.make("references.ToptierAgency", toptier_code="007", _fill_optional=True)

    baker.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, _fill_optional=True)

    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )

    fa1 = baker.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")
    tas1 = baker.make(
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

    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)


__all__ = [
    "tas_with_no_object_class",
    "agency_account_data",
    "tas_mulitple_pas_per_oc",
    "helpers",
    "bureau_data",
]
