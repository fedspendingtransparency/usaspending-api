import pytest

from model_mommy import mommy

from usaspending_api.references.models.disaster_emergency_fund_code import DisasterEmergencyFundCode


@pytest.fixture
def generic_account_data():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="2020-06-15",
        period_start_date="2022-04-01",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        is_quarter=True,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="2020-06-15",
        period_start_date="2022-04-01",
    )
    mommy.make("references.DisasterEmergencyFundCode", code="P")
    mommy.make("references.DisasterEmergencyFundCode", code="A")
    defc_l = mommy.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")
    defc_m = mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")
    defc_n = mommy.make("references.DisasterEmergencyFundCode", code="N", group_name="covid_19")
    defc_o = mommy.make("references.DisasterEmergencyFundCode", code="O", group_name="covid_19")
    defc_9 = mommy.make("references.DisasterEmergencyFundCode", code="9")
    award1 = mommy.make("awards.Award", id=111, total_loan_value=1111, type="A")
    award2 = mommy.make("awards.Award", id=222, total_loan_value=2222, type="A")
    award3 = mommy.make("awards.Award", id=333, total_loan_value=3333, type="07")
    award4 = mommy.make("awards.Award", id=444, total_loan_value=4444, type="08")
    fed_acct1 = mommy.make("accounts.FederalAccount", account_title="gifts", federal_account_code="000-0000", id=21)
    tre_acct1 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/99",
        account_title="flowers",
        treasury_account_identifier=22,
    )
    tre_acct2 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/98",
        account_title="evergreens",
        treasury_account_identifier=23,
    )
    tre_acct3 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/52",
        account_title="ferns",
        treasury_account_identifier=24,
    )
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_period_start="2022-05-15",
        reporting_period_end="2022-05-29",
        reporting_fiscal_year=2022,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=7,
        is_final_balances_for_fy=True,
        quarter_format_flag=False,
        submission_window_id=11,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=111,
        disaster_emergency_fund=defc_m,
        treasury_account=tre_acct1,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=200,
        gross_outlay_amount_by_program_object_class_cpe=222,
        disaster_emergency_fund=defc_l,
        treasury_account=tre_acct2,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=2,
        gross_outlay_amount_by_program_object_class_cpe=2,
        disaster_emergency_fund=defc_9,
        treasury_account=tre_acct2,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=1,
        disaster_emergency_fund=defc_o,
        treasury_account=tre_acct2,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=3,
        gross_outlay_amount_by_program_object_class_cpe=333,
        disaster_emergency_fund=defc_n,
        treasury_account=tre_acct3,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        piid="0wefjwe",
        award_id=111,
        parent_award_id="3443r",
        transaction_obligated_amount=100,
        gross_outlay_amount_by_award_cpe=111,
        disaster_emergency_fund=defc_m,
        treasury_account=tre_acct1,
        distinct_award_key="0wefjwe|3443r||",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award1,
        piid="0wefjwe",
        award_id=111,
        parent_award_id="3443r",
        transaction_obligated_amount=200,
        gross_outlay_amount_by_award_cpe=222,
        disaster_emergency_fund=defc_l,
        treasury_account=tre_acct2,
        distinct_award_key="0wefjwe|3443r||",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award2,
        uri="3298rhed",
        award_id=222,
        transaction_obligated_amount=2,
        gross_outlay_amount_by_award_cpe=2,
        disaster_emergency_fund=defc_9,
        treasury_account=tre_acct2,
        distinct_award_key="|||3298rhed",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award3,
        fain="43tgfvdvfv",
        award_id=333,
        transaction_obligated_amount=1,
        gross_outlay_amount_by_award_cpe=1,
        disaster_emergency_fund=defc_o,
        treasury_account=tre_acct2,
        distinct_award_key="||43tgfvdvfv|",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award4,
        fain="woefhowe",
        award_id=444,
        transaction_obligated_amount=3,
        gross_outlay_amount_by_award_cpe=333,
        disaster_emergency_fund=defc_n,
        treasury_account=tre_acct3,
        distinct_award_key="||woefhowe|",
    )
    mommy.make(
        "references.GTASSF133Balances",
        budget_authority_appropriation_amount_cpe=4358.0,
        fiscal_year=2022,
        fiscal_period=7,
        disaster_emergency_fund_code="M",
        treasury_account_identifier=tre_acct1,
        total_budgetary_resources_cpe=43580.0,
    )
    mommy.make(
        "references.GTASSF133Balances",
        budget_authority_appropriation_amount_cpe=109237.0,
        fiscal_year=2022,
        fiscal_period=7,
        disaster_emergency_fund_code="M",
        treasury_account_identifier=tre_acct2,
        total_budgetary_resources_cpe=1092370.0,
    )
    mommy.make(
        "references.GTASSF133Balances",
        budget_authority_appropriation_amount_cpe=39248.0,
        fiscal_year=2022,
        fiscal_period=7,
        disaster_emergency_fund_code="M",
        treasury_account_identifier=tre_acct3,
        total_budgetary_resources_cpe=392480.0,
    )


@pytest.fixture
def unlinked_faba_account_data():
    fed_acct = mommy.make("accounts.FederalAccount", account_title="soap", federal_account_code="999-0000", id=99)
    tre_acct = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct,
        tas_rendering_label="2020/99",
        account_title="dove",
        treasury_account_identifier=99,
        gtas__budget_authority_appropriation_amount_cpe=9939248,
    )
    sub = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_period_start="2022-05-15",
        reporting_period_end="2022-05-29",
        reporting_fiscal_year=2022,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=7,
        is_final_balances_for_fy=True,
        quarter_format_flag=False,
        submission_window_id=11,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub,
        piid="weuf",
        transaction_obligated_amount=999999,
        gross_outlay_amount_by_award_cpe=9999999,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.get(code="N"),
        treasury_account=tre_acct,
        distinct_award_key="weuf|||",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub,
        piid="weuf",
        parent_award_id="weuf22",
        transaction_obligated_amount=88888,
        gross_outlay_amount_by_award_cpe=888888,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.get(code="N"),
        treasury_account=tre_acct,
        distinct_award_key="weuf|weuf22||",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub,
        piid="0iwnff",
        transaction_obligated_amount=5,
        gross_outlay_amount_by_award_cpe=555,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.get(code="L"),
        treasury_account=tre_acct,
        distinct_award_key="0iwnff|||",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub,
        fain="howeusd",
        transaction_obligated_amount=8,
        gross_outlay_amount_by_award_cpe=888,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.get(code="N"),
        treasury_account=tre_acct,
        distinct_award_key="||howeusd|",
    )
