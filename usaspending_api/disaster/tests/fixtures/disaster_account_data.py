import pytest

from model_mommy import mommy


@pytest.fixture
def disaster_account_data():
    ta1 = mommy.make("references.ToptierAgency", toptier_agency_id=7, toptier_code="007", name="Agency 007")
    ta2 = mommy.make("references.ToptierAgency", toptier_agency_id=8, toptier_code="008", name="Agency 008")
    ta3 = mommy.make("references.ToptierAgency", toptier_agency_id=9, toptier_code="009", name="Agency 009")
    ta4 = mommy.make("references.ToptierAgency", toptier_agency_id=10, toptier_code="010", name="Agency 010")

    sa1 = mommy.make("references.SubtierAgency", subtier_agency_id=1007, subtier_code="1007", name="Subtier 1007")
    sa2 = mommy.make("references.SubtierAgency", subtier_agency_id=1008, subtier_code="1008", name="Subtier 1008")
    sa3 = mommy.make("references.SubtierAgency", subtier_agency_id=2008, subtier_code="2008", name="Subtier 2008")
    sa4 = mommy.make("references.SubtierAgency", subtier_agency_id=3008, subtier_code="3008", name="Subtier 3008")

    ag1 = mommy.make("references.Agency", id=1, toptier_agency=ta1, subtier_agency=sa1, toptier_flag=True)
    ag2 = mommy.make("references.Agency", id=2, toptier_agency=ta2, subtier_agency=sa2, toptier_flag=True)
    ag3 = mommy.make("references.Agency", id=3, toptier_agency=ta2, subtier_agency=sa3)
    mommy.make("references.Agency", id=4, toptier_agency=ta3, subtier_agency=sa4, toptier_flag=True)

    dsws1 = mommy.make(
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
    dsws2 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2022071,
        is_quarter=True,
        period_start_date="2022-04-01",
        period_end_date="2022-04-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2022-4-15",
    )
    dsws3 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2022080,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    # Unclosed submisssion window
    dsws4 = mommy.make(
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
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=True,
        is_final_balances_for_fy=False,
        reporting_period_start="2022-04-01",
        toptier_code="007",
        submission_window=dsws2,
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        toptier_code="008",
        submission_window=dsws3,
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=7,
        quarter_format_flag=True,
        is_final_balances_for_fy=False,
        reporting_period_start="2022-04-01",
        toptier_code="009",
        submission_window=dsws2,
    )
    sub4 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window=dsws3,
    )
    sub5 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="2019-04-01",
        submission_window=dsws1,
    )
    sub6 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=9999,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="9999-04-01",
        submission_window=dsws4,
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

    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3, submission=sub4)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas4, submission=sub5)

    pa1 = mommy.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = mommy.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")
    pa3 = mommy.make("references.RefProgramActivity", program_activity_code="4567", program_activity_name="NAME 3")
    pa4 = mommy.make("references.RefProgramActivity", program_activity_code="111", program_activity_name="NAME 4")
    pa5 = mommy.make("references.RefProgramActivity", program_activity_code="1234", program_activity_name="NAME 5")

    oc = "references.ObjectClass"
    oc1 = mommy.make(
        oc,
        major_object_class=10,
        major_object_class_name="Name 10",
        object_class=100,
        object_class_name="equipment",
        direct_reimbursable="R",
    )
    oc2 = mommy.make(
        oc,
        major_object_class=20,
        major_object_class_name="Name 20",
        object_class=110,
        object_class_name="hvac",
        direct_reimbursable="R",
    )
    oc3 = mommy.make(
        oc,
        major_object_class=30,
        major_object_class_name="Name 30",
        object_class=120,
        object_class_name="supplies",
        direct_reimbursable="R",
    )
    oc4 = mommy.make(
        oc,
        major_object_class=40,
        major_object_class_name="Name 40",
        object_class=130,
        object_class_name="interest",
        direct_reimbursable="R",
    )
    oc5 = mommy.make(
        oc,
        major_object_class=40,
        major_object_class_name="Name 40",
        object_class=140,
        object_class_name="interest",
        direct_reimbursable="R",
    )
    oc6 = mommy.make(
        oc,
        major_object_class=30,
        major_object_class_name="Name 30",
        object_class=120,
        object_class_name="supplies",
        direct_reimbursable="D",
    )

    defc = "references.DisasterEmergencyFundCode"
    defc_l = mommy.make(
        defc, code="L", public_law="PUBLIC LAW FOR CODE L", title="TITLE FOR CODE L", group_name="covid_19"
    )
    defc_m = mommy.make(
        defc, code="M", public_law="PUBLIC LAW FOR CODE M", title="TITLE FOR CODE M", group_name="covid_19"
    )
    defc_n = mommy.make(
        defc, code="N", public_law="PUBLIC LAW FOR CODE N", title="TITLE FOR CODE N", group_name="covid_19"
    )
    defc_o = mommy.make(
        defc, code="O", public_law="PUBLIC LAW FOR CODE O", title="TITLE FOR CODE O", group_name="covid_19"
    )
    defc_p = mommy.make(
        defc, code="P", public_law="PUBLIC LAW FOR CODE P", title="TITLE FOR CODE P", group_name="covid_19"
    )
    mommy.make(defc, code="9", public_law="PUBLIC LAW FOR CODE 9", title="TITLE FOR CODE 9")

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        disaster_emergency_fund=defc_l,
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
        disaster_emergency_fund=defc_m,
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
        disaster_emergency_fund=defc_p,
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
        disaster_emergency_fund=defc_o,
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
        disaster_emergency_fund=defc_n,
        obligations_incurred_by_program_object_class_cpe=10000,
        gross_outlay_amount_by_program_object_class_cpe=1000,
    )
    mommy.make(
        fabpaoc,
        final_of_fy=True,
        treasury_account=tas3,
        submission=sub3,
        program_activity=pa4,
        object_class=oc6,
        disaster_emergency_fund=defc_n,
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
        disaster_emergency_fund=defc_o,
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
        disaster_emergency_fund=defc_o,
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
        disaster_emergency_fund=defc_p,
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

    a1 = mommy.make(
        "awards.Award", id=1, total_loan_value=333, type="07", funding_agency=ag1, latest_transaction_id=10
    )  # Loan
    a2 = mommy.make(
        "awards.Award", id=2, total_loan_value=444, type="02", funding_agency=ag2, latest_transaction_id=20
    )  # Block Grant - subtier sister to a4
    a3 = mommy.make(
        "awards.Award", id=3, total_loan_value=444, type="A", funding_agency=ag3, latest_transaction_id=30
    )  # BPA Call
    a4 = mommy.make(
        "awards.Award", id=4, total_loan_value=555, type="02", funding_agency=ag3, latest_transaction_id=40
    )  # Block Grant - subtier sister to a2

    mommy.make(
        "awards.TransactionNormalized", id=10, award=a1, action_date="2020-04-01", is_fpds=False, funding_agency=ag1
    )
    mommy.make(
        "awards.TransactionNormalized", id=20, award=a2, action_date="2020-04-02", is_fpds=False, funding_agency=ag2
    )
    mommy.make(
        "awards.TransactionNormalized", id=30, award=a3, action_date="2020-04-03", is_fpds=True, funding_agency=ag3
    )
    mommy.make(
        "awards.TransactionNormalized", id=40, award=a4, action_date="2020-04-04", is_fpds=False, funding_agency=ag3
    )

    mommy.make("awards.TransactionFABS", transaction_id=10)
    mommy.make("awards.TransactionFABS", transaction_id=20)
    mommy.make("awards.TransactionFPDS", transaction_id=30)
    mommy.make("awards.TransactionFABS", transaction_id=40)

    faba = "awards.FinancialAccountsByAwards"
    mommy.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_l,
        transaction_obligated_amount=2,
        gross_outlay_amount_by_award_cpe=20000000,
    )
    mommy.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_m,
        transaction_obligated_amount=20,
        gross_outlay_amount_by_award_cpe=2000000,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas1,
        submission=sub1,
        disaster_emergency_fund=defc_p,
        transaction_obligated_amount=200,
        gross_outlay_amount_by_award_cpe=200000,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas2,
        submission=sub2,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=1000,
        gross_outlay_amount_by_award_cpe=10000,
        award=a1,
    )
    mommy.make(
        faba,
        treasury_account=tas2,
        submission=sub2,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=1000,
        gross_outlay_amount_by_award_cpe=10000,
        award=a1,
    )
    mommy.make(
        faba,
        treasury_account=tas2,
        submission=sub3,
        disaster_emergency_fund=defc_n,
        transaction_obligated_amount=20000,
        gross_outlay_amount_by_award_cpe=2000,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas3,
        submission=sub3,
        disaster_emergency_fund=defc_n,
        transaction_obligated_amount=200000,
        gross_outlay_amount_by_award_cpe=200,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=2000000,
        gross_outlay_amount_by_award_cpe=20,
        award=a2,
    )
    mommy.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=20000000,
        gross_outlay_amount_by_award_cpe=2,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas4,
        submission=sub5,
        disaster_emergency_fund=defc_p,
        transaction_obligated_amount=0,
        gross_outlay_amount_by_award_cpe=0,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas5,
        submission=sub1,
        disaster_emergency_fund=None,
        transaction_obligated_amount=20,
        gross_outlay_amount_by_award_cpe=2000000,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas6,
        submission=sub1,
        disaster_emergency_fund=None,
        transaction_obligated_amount=200,
        gross_outlay_amount_by_award_cpe=200000,
        award=a3,
    )
    mommy.make(
        faba,
        treasury_account=tas3,
        submission=sub4,
        disaster_emergency_fund=defc_o,
        transaction_obligated_amount=-2,
        gross_outlay_amount_by_award_cpe=200000000,
        award=a4,
    )
    mommy.make(
        faba,
        treasury_account=tas1,
        submission=sub6,
        disaster_emergency_fund=defc_l,
        transaction_obligated_amount=80,
        gross_outlay_amount_by_award_cpe=20,
        award=a1,
    )

    mommy.make(
        "references.GTASSF133Balances",
        disaster_emergency_fund_code="M",
        tas_rendering_label="003-X-0000-000",
        treasury_account_identifier=tas3,
        budget_authority_appropriation_amount_cpe=2398472389.78,
        total_budgetary_resources_cpe=23984723890.78,
        fiscal_period=8,
        fiscal_year=2022,
    )
    mommy.make(
        "references.GTASSF133Balances",
        disaster_emergency_fund_code="N",
        tas_rendering_label="002-X-0000-000",
        treasury_account_identifier=tas2,
        budget_authority_appropriation_amount_cpe=892743123.12,
        total_budgetary_resources_cpe=8927431230.12,
        fiscal_period=8,
        fiscal_year=2022,
    )
