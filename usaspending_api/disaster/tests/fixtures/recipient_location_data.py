import pytest

from model_mommy import mommy


@pytest.fixture
def awards_and_transactions():
    # Awards
    award1 = mommy.make("awards.Award", latest_transaction_id=10, type="07", total_loan_value=3)
    award2 = mommy.make("awards.Award", latest_transaction_id=20, type="07", total_loan_value=30)
    award3 = mommy.make("awards.Award", latest_transaction_id=30, type="08", total_loan_value=300)
    award4 = mommy.make("awards.Award", latest_transaction_id=50, type="B", total_loan_value=0)
    award5 = mommy.make("awards.Award", latest_transaction_id=40, type="A", total_loan_value=0)
    award6 = mommy.make("awards.Award", latest_transaction_id=60, type="C", total_loan_value=0)
    award7 = mommy.make("awards.Award", latest_transaction_id=70, type="D", total_loan_value=0)

    # Disaster Emergency Fund Code
    defc1 = mommy.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
    )
    defc2 = mommy.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    mommy.make(
        "references.DisasterEmergencyFundCode",
        code="N",
        public_law="PUBLIC LAW FOR CODE N",
        title="TITLE FOR CODE N",
        group_name="covid_19",
    )

    # Submission Attributes
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )
    sub2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )
    sub3 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )

    # Financial Accounts by Awards
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=1,
        award=award1,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=2,
        award=award2,
        submission=sub1,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=10,
        transaction_obligated_amount=20,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=3,
        award=award3,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=100,
        transaction_obligated_amount=200,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=4,
        award=award4,
        submission=sub2,
        disaster_emergency_fund=defc1,
        gross_outlay_amount_by_award_cpe=1000,
        transaction_obligated_amount=2000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=5,
        award=award5,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=10000,
        transaction_obligated_amount=20000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=6,
        award=award6,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=100000,
        transaction_obligated_amount=200000,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        pk=7,
        award=award7,
        submission=sub3,
        disaster_emergency_fund=defc2,
        gross_outlay_amount_by_award_cpe=1000000,
        transaction_obligated_amount=2000000,
    )

    # DABS Submission Window Schedule
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022081",
        is_quarter=False,
        period_start_date="2022-05-01",
        period_end_date="2022-05-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022080",
        is_quarter=True,
        period_start_date="2022-05-01",
        period_end_date="2022-05-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )

    # Transaction Normalized
    mommy.make(
        "awards.TransactionNormalized",
        id=10,
        award=award1,
        federal_action_obligation=5,
        action_date="2022-01-01",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=20,
        award=award2,
        federal_action_obligation=50,
        action_date="2022-01-02",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=30,
        award=award3,
        federal_action_obligation=500,
        action_date="2022-01-03",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=40,
        award=award4,
        federal_action_obligation=5000,
        action_date="2022-01-04",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=50,
        award=award5,
        federal_action_obligation=50000,
        action_date="2022-01-05",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=60,
        award=award6,
        federal_action_obligation=500000,
        action_date="2022-01-06",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=70,
        award=award7,
        federal_action_obligation=5000000,
        action_date="2022-01-07",
        is_fpds=True,
    )

    # Transaction FABS
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=10,
        cfda_number="10.100",
        place_of_perform_country_c="USA",
        place_of_perfor_state_code=None,
        place_of_perform_county_co=None,
        place_of_perform_county_na=None,
        place_of_performance_congr=None,
        legal_entity_country_code=None,
        legal_entity_state_code=None,
        legal_entity_county_code=None,
        legal_entity_county_name=None,
        legal_entity_congressional=None,
        awardee_or_recipient_legal="RECIPIENT 1",
        awardee_or_recipient_uniqu=None,
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=20,
        cfda_number="20.200",
        place_of_perform_country_c="USA",
        place_of_perfor_state_code="SC",
        place_of_perform_county_co="005",
        place_of_perform_county_na="TEST NAME",
        place_of_performance_congr="50",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="001",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="90",
        awardee_or_recipient_legal="RECIPIENT 2",
        awardee_or_recipient_uniqu="456789123",
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=30,
        cfda_number="20.200",
        place_of_perform_country_c="USA",
        place_of_perfor_state_code="WA",
        place_of_perform_county_co="005",
        place_of_perform_county_na="TEST NAME",
        place_of_performance_congr="50",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="001",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="50",
        awardee_or_recipient_legal="RECIPIENT, 3",
        awardee_or_recipient_uniqu="987654321",
    )

    # Transaction FPDS
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=40,
        place_of_perform_country_c="USA",
        place_of_performance_state="WA",
        place_of_perform_county_co="005",
        place_of_perform_county_na="TEST NAME",
        place_of_performance_congr="50",
        legal_entity_country_code="USA",
        legal_entity_state_code="WA",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu="096354360",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=50,
        place_of_perform_country_c="USA",
        place_of_performance_state="SC",
        place_of_perform_county_co="001",
        place_of_perform_county_na="CHARLESTON",
        place_of_performance_congr="10",
        legal_entity_country_code="USA",
        legal_entity_state_code="WA",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal=None,
        awardee_or_recipient_uniqu="987654321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=60,
        place_of_perform_country_c="USA",
        place_of_performance_state="SC",
        place_of_perform_county_co="001",
        place_of_perform_county_na="CHARLESTON",
        place_of_performance_congr="90",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="005",
        legal_entity_county_name="TEST NAME",
        legal_entity_congressional="50",
        awardee_or_recipient_legal=None,
        awardee_or_recipient_uniqu="987654321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=70,
        place_of_perform_country_c="USA",
        place_of_performance_state="SC",
        place_of_perform_county_co="001",
        place_of_perform_county_na="CHARLESTON",
        place_of_performance_congr="90",
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="01",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="10",
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu=None,
    )

    # References State Data
    mommy.make("recipient.StateData", id="45-2022", fips="45", code="SC", name="South Carolina")
    mommy.make("recipient.StateData", id="53-2022", fips="53", code="WA", name="Washington")

    # References Country
    mommy.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")

    # References Population County
    mommy.make(
        "references.PopCounty",
        id=1,
        state_code="45",
        state_name="South Carolina",
        county_number="001",
        latest_population=1,
    )
    mommy.make(
        "references.PopCounty",
        id=2,
        state_code="45",
        state_name="South Carolina",
        county_number="005",
        latest_population=10,
    )
    mommy.make(
        "references.PopCounty",
        id=3,
        state_code="53",
        state_name="Washington",
        county_number="005",
        latest_population=100,
    )
    mommy.make(
        "references.PopCounty",
        id=4,
        state_code="45",
        state_name="South Carolina",
        county_number="000",
        latest_population=1000,
    )
    mommy.make(
        "references.PopCounty",
        id=5,
        state_code="53",
        state_name="Washington",
        county_number="000",
        latest_population=10000,
    )

    # References Population Congressional District
    mommy.make(
        "references.PopCongressionalDistrict",
        id=1,
        state_code="45",
        state_name="South Carolina",
        congressional_district="90",
        latest_population=1,
    )
    mommy.make(
        "references.PopCongressionalDistrict",
        id=2,
        state_code="45",
        state_name="South Carolina",
        congressional_district="10",
        latest_population=10,
    )
    mommy.make(
        "references.PopCongressionalDistrict",
        id=2,
        state_code="45",
        state_name="South Carolina",
        congressional_district="50",
        latest_population=100,
    )
    mommy.make(
        "references.PopCongressionalDistrict",
        id=3,
        state_code="53",
        state_name="Washington",
        congressional_district="50",
        latest_population=1000,
    )

    # Recipient Profile
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 1",
        recipient_level="R",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_unique_id=None,
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 2",
        recipient_level="R",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        recipient_unique_id="456789123",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="P",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="C",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_level="R",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        recipient_unique_id="096354360",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="R",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        recipient_unique_id="987654321",
    )

    # Recipient Lookup
    mommy.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT, 3",
        recipient_hash="d2894d22-67fc-f9cb-4005-33fa6a29ef86",
        duns="987654321",
    )

    # Ref Country Code
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    mommy.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
