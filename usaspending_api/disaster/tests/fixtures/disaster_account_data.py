import pytest

from model_bakery import baker
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_date


@pytest.fixture
def disaster_account_data():
    ta1 = baker.make("references.ToptierAgency", toptier_agency_id=7, toptier_code="007", name="Agency 007")
    ta2 = baker.make("references.ToptierAgency", toptier_agency_id=8, toptier_code="008", name="Agency 008")
    ta3 = baker.make("references.ToptierAgency", toptier_agency_id=9, toptier_code="009", name="Agency 009")
    ta4 = baker.make("references.ToptierAgency", toptier_agency_id=10, toptier_code="010", name="Agency 010")

    sa1 = baker.make("references.SubtierAgency", subtier_agency_id=1007, subtier_code="1007", name="Subtier 1007")
    sa2 = baker.make("references.SubtierAgency", subtier_agency_id=1008, subtier_code="1008", name="Subtier 1008")
    sa3 = baker.make("references.SubtierAgency", subtier_agency_id=2008, subtier_code="2008", name="Subtier 2008")
    sa4 = baker.make("references.SubtierAgency", subtier_agency_id=3008, subtier_code="3008", name="Subtier 3008")

    ag1 = baker.make("references.Agency", id=1, toptier_agency=ta1, subtier_agency=sa1, toptier_flag=True)
    ag2 = baker.make("references.Agency", id=2, toptier_agency=ta2, subtier_agency=sa2, toptier_flag=True)
    ag3 = baker.make("references.Agency", id=3, toptier_agency=ta2, subtier_agency=sa3, toptier_flag=False)
    ag4 = baker.make("references.Agency", id=4, toptier_agency=ta3, subtier_agency=sa4, toptier_flag=True)

    dsws1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2019070,
        is_quarter=False,
        period_start_date="2022-04-01",
        period_end_date="2022-04-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="2020-4-15",
    )
    dsws2 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2022071,
        is_quarter=True,
        period_start_date="2022-04-01",
        period_end_date="2022-04-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date=f"{current_fiscal_date()}",
    )
    dsws3 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2022080,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    # Unclosed submission window
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2021120,
        is_quarter=False,
        submission_fiscal_year=2021,
        submission_fiscal_quarter=4,
        submission_fiscal_month=12,
        submission_reveal_date="2021-11-17",
        period_start_date="2021-09-01",
    )
    # Unclosed submisssion window
    dsws4 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=9999070,
        is_quarter=False,
        period_start_date="9999-04-01",
        period_end_date="9999-04-30",
        submission_fiscal_year=9999,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="9999-4-15",
    )
    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=True,
        is_final_balances_for_fy=False,
        reporting_period_start="2022-04-01",
        toptier_code="007",
        submission_window=dsws2,
    )
    sub2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        toptier_code="008",
        submission_window=dsws3,
    )
    sub3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=True,
        is_final_balances_for_fy=False,
        reporting_period_start="2022-04-01",
        toptier_code="009",
        submission_window=dsws2,
    )
    sub4 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window=dsws3,
    )
    sub5 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="2019-04-01",
        submission_window=dsws1,
    )
    sub6 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=9999,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="9999-04-01",
        submission_window=dsws4,
    )

    fa1 = baker.make(
        "accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1", parent_toptier_agency=ta1
    )
    fa2 = baker.make(
        "accounts.FederalAccount", federal_account_code="002-0000", account_title="FA 2", parent_toptier_agency=ta2
    )
    fa3 = baker.make(
        "accounts.FederalAccount", federal_account_code="003-0000", account_title="FA 3", parent_toptier_agency=ta3
    )
    fa4 = baker.make(
        "accounts.FederalAccount", federal_account_code="004-0000", account_title="FA 4", parent_toptier_agency=ta4
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

    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub2)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3, submission=sub4)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas4, submission=sub5)

    pa1 = baker.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = baker.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")
    pa3 = baker.make("references.RefProgramActivity", program_activity_code="4567", program_activity_name="NAME 3")
    pa4 = baker.make("references.RefProgramActivity", program_activity_code="111", program_activity_name="NAME 4")
    pa5 = baker.make("references.RefProgramActivity", program_activity_code="1234", program_activity_name="NAME 5")

    oc = "references.ObjectClass"
    oc1 = baker.make(
        oc,
        major_object_class=10,
        major_object_class_name="Name 10",
        object_class=100,
        object_class_name="equipment",
        direct_reimbursable="R",
    )
    oc2 = baker.make(
        oc,
        major_object_class=20,
        major_object_class_name="Name 20",
        object_class=110,
        object_class_name="hvac",
        direct_reimbursable="R",
    )
    oc3 = baker.make(
        oc,
        major_object_class=30,
        major_object_class_name="Name 30",
        object_class=120,
        object_class_name="supplies",
        direct_reimbursable="R",
    )
    oc4 = baker.make(
        oc,
        major_object_class=40,
        major_object_class_name="Name 40",
        object_class=130,
        object_class_name="interest",
        direct_reimbursable="R",
    )
    oc5 = baker.make(
        oc,
        major_object_class=40,
        major_object_class_name="Name 40",
        object_class=140,
        object_class_name="interest",
        direct_reimbursable="R",
    )
    oc6 = baker.make(
        oc,
        major_object_class=30,
        major_object_class_name="Name 30",
        object_class=120,
        object_class_name="supplies",
        direct_reimbursable="D",
    )

    defc = "references.DisasterEmergencyFundCode"
    defc_l = baker.make(
        defc, code="L", public_law="PUBLIC LAW FOR CODE L", title="TITLE FOR CODE L", group_name="covid_19"
    )
    defc_m = baker.make(
        defc, code="M", public_law="PUBLIC LAW FOR CODE M", title="TITLE FOR CODE M", group_name="covid_19"
    )
    defc_n = baker.make(
        defc, code="N", public_law="PUBLIC LAW FOR CODE N", title="TITLE FOR CODE N", group_name="covid_19"
    )
    defc_o = baker.make(
        defc, code="O", public_law="PUBLIC LAW FOR CODE O", title="TITLE FOR CODE O", group_name="covid_19"
    )
    defc_p = baker.make(
        defc, code="P", public_law="PUBLIC LAW FOR CODE P", title="TITLE FOR CODE P", group_name="covid_19"
    )
    baker.make(defc, code="9", public_law="PUBLIC LAW FOR CODE 9", title="TITLE FOR CODE 9")
    defc_q = baker.make(
        defc, code="Q", public_law="PUBLIC LAW FOR CODE Q", title="TITLE FOR CODE Q", group_name="covid_19"
    )
    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        disaster_emergency_fund=defc_l,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=9850000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc2,
        disaster_emergency_fund=defc_m,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=985000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-90,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=5000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=10000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa3,
        object_class=oc3,
        disaster_emergency_fund=defc_p,
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=98500,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-900,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=500,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=1000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub2,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc_o,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=9850,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9000,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=50,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=100,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas2,
        submission=sub3,
        program_activity=pa4,
        object_class=oc3,
        disaster_emergency_fund=defc_n,
        obligations_incurred_by_program_object_class_cpe=100000,
        gross_outlay_amount_by_program_object_class_cpe=985,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-90000,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=5,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=10,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub3,
        program_activity=pa4,
        object_class=oc6,
        disaster_emergency_fund=defc_n,
        obligations_incurred_by_program_object_class_cpe=1000000,
        gross_outlay_amount_by_program_object_class_cpe=98.5,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-900000,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.5,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=1,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc_o,
        obligations_incurred_by_program_object_class_cpe=10000000,
        gross_outlay_amount_by_program_object_class_cpe=8.5,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-9000000,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.5,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=1,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas3,
        submission=sub4,
        program_activity=pa4,
        object_class=oc4,
        disaster_emergency_fund=defc_o,
        obligations_incurred_by_program_object_class_cpe=100000000,
        gross_outlay_amount_by_program_object_class_cpe=0.85,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-90000000,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0.05,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0.1,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas4,
        submission=sub5,
        program_activity=pa5,
        object_class=oc5,
        disaster_emergency_fund=defc_p,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-50,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-50,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas5,
        submission=sub1,
        disaster_emergency_fund=None,
        obligations_incurred_by_program_object_class_cpe=5,
        gross_outlay_amount_by_program_object_class_cpe=60000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-5,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=930000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=1000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas6,
        submission=sub1,
        disaster_emergency_fund=None,
        obligations_incurred_by_program_object_class_cpe=125,
        gross_outlay_amount_by_program_object_class_cpe=50000,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-25,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=25000,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=25000,
    )

    a1 = baker.make(
        "awards.Award", id=1, total_loan_value=333, type="07", funding_agency=ag1, latest_transaction_id=10
    )  # Loan
    a2 = baker.make(
        "awards.Award", id=2, total_loan_value=444, type="02", funding_agency=ag2, latest_transaction_id=20
    )  # Block Grant - subtier sister to a4
    a3 = baker.make(
        "awards.Award", id=3, total_loan_value=444, type="A", funding_agency=ag3, latest_transaction_id=30
    )  # BPA Call
    a4 = baker.make(
        "awards.Award", id=4, total_loan_value=555, type="02", funding_agency=ag3, latest_transaction_id=40
    )  # Block Grant - subtier sister to a2
    a5 = baker.make("awards.Award", id=5, total_loan_value=666, type="02", funding_agency=ag4, latest_transaction_id=50)

    baker.make(
        "awards.TransactionNormalized", id=10, award=a1, action_date="2020-04-01", is_fpds=False, funding_agency=ag1
    )
    baker.make(
        "awards.TransactionNormalized", id=20, award=a2, action_date="2020-04-02", is_fpds=False, funding_agency=ag2
    )
    baker.make(
        "awards.TransactionNormalized", id=30, award=a3, action_date="2020-04-03", is_fpds=True, funding_agency=ag3
    )
    baker.make(
        "awards.TransactionNormalized", id=40, award=a4, action_date="2020-04-04", is_fpds=False, funding_agency=ag3
    )
    baker.make(
        "awards.TransactionNormalized", id=50, award=a5, action_date="2020-04-04", is_fpds=False, funding_agency=ag4
    )

    baker.make("awards.TransactionFABS", transaction_id=10)
    baker.make("awards.TransactionFABS", transaction_id=20)
    baker.make("awards.TransactionFPDS", transaction_id=30)
    baker.make("awards.TransactionFABS", transaction_id=40)
    baker.make("awards.TransactionFABS", transaction_id=50)

    faba = "awards.FinancialAccountsByAwards"
    baker.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_l,
        transaction_obligated_amount=2,
        gross_outlay_amount_by_award_cpe=20000000,
        distinct_award_key=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_m,
        transaction_obligated_amount=20,
        gross_outlay_amount_by_award_cpe=2000000,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_p,
        transaction_obligated_amount=200,
        gross_outlay_amount_by_award_cpe=200000,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas2,
        submission=sub2,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=1000,
        gross_outlay_amount_by_award_cpe=10000,
        award=a1,
        distinct_award_key=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas2,
        submission=sub2,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=1000,
        gross_outlay_amount_by_award_cpe=10000,
        award=a1,
        distinct_award_key=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas2,
        submission=sub3,
        disaster_emergency_fund=defc_n,
        transaction_obligated_amount=20000,
        gross_outlay_amount_by_award_cpe=2000,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas3,
        submission=sub3,
        disaster_emergency_fund=defc_n,
        transaction_obligated_amount=200000,
        gross_outlay_amount_by_award_cpe=200,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=2000000,
        gross_outlay_amount_by_award_cpe=20,
        award=a2,
        distinct_award_key=2,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=20000000,
        gross_outlay_amount_by_award_cpe=2,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_q,
        transaction_obligated_amount=2,
        gross_outlay_amount_by_award_cpe=2,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas4,
        submission=sub5,
        disaster_emergency_fund=defc_p,
        transaction_obligated_amount=0,
        gross_outlay_amount_by_award_cpe=0,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas5,
        submission=sub1,
        disaster_emergency_fund=None,
        transaction_obligated_amount=20,
        gross_outlay_amount_by_award_cpe=2000000,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas6,
        submission=sub1,
        disaster_emergency_fund=None,
        transaction_obligated_amount=200,
        gross_outlay_amount_by_award_cpe=200000,
        award=a3,
        distinct_award_key=3,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=-2,
        gross_outlay_amount_by_award_cpe=200000000,
        award=a4,
        distinct_award_key=4,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas1,
        submission=sub6,
        disaster_emergency_fund=defc_l,
        transaction_obligated_amount=80,
        gross_outlay_amount_by_award_cpe=20,
        award=a1,
        distinct_award_key=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )
    baker.make(
        faba,
        treasury_account=tas4,
        submission=sub4,
        disaster_emergency_fund=defc_l,
        transaction_obligated_amount=1000,
        gross_outlay_amount_by_award_cpe=1000,
        award=a5,
        distinct_award_key=5,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )

    baker.make(
        "references.GTASSF133Balances",
        disaster_emergency_fund=defc_m,
        tas_rendering_label="003-X-0000-000",
        treasury_account_identifier=tas3,
        budget_authority_appropriation_amount_cpe=2398472389.78,
        total_budgetary_resources_cpe=23986200349.56,
        budget_authority_unobligated_balance_brought_forward_cpe=1000,
        deobligations_or_recoveries_or_refunds_from_prior_year_cpe=1238972.78,
        prior_year_paid_obligation_recoveries=237486,
        fiscal_period=12,
        fiscal_year=2021,
    )
    baker.make(
        "references.GTASSF133Balances",
        disaster_emergency_fund=defc_n,
        tas_rendering_label="002-X-0000-000",
        treasury_account_identifier=tas2,
        budget_authority_appropriation_amount_cpe=892743123.12,
        total_budgetary_resources_cpe=8927768300.12,
        budget_authority_unobligated_balance_brought_forward_cpe=2000,
        deobligations_or_recoveries_or_refunds_from_prior_year_cpe=238746,
        prior_year_paid_obligation_recoveries=98324,
        fiscal_period=8,
        fiscal_year=2022,
    )
