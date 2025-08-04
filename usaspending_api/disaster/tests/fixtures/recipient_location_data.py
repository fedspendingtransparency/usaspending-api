import pytest

from model_bakery import baker


@pytest.fixture
def awards_and_transactions():
    # Awards
    award1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=10,
        type="07",
        total_loan_value=3,
        action_date="2022-01-01",
        is_fpds=False,
        cfda_number="10.100",
        pop_country_code="USA",
        pop_state_code=None,
        pop_county_code=None,
        pop_county_name=None,
        pop_congressional_code=None,
        pop_congressional_code_current=None,
        recipient_location_country_code=None,
        recipient_location_state_code=None,
        recipient_location_county_code=None,
        recipient_location_county_name=None,
        recipient_location_congressional_code=None,
        recipient_location_congressional_code_current=None,
        recipient_name="RECIPIENT 1",
        recipient_unique_id=None,
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_levels=["R"],
        disaster_emergency_fund_codes=["L"],
        total_covid_outlay=1,
        total_covid_obligation=2,
        spending_by_defc=[{"defc": "L", "outlay": 1, "obligation": 2}],
    )
    award2 = baker.make(
        "search.AwardSearch",
        award_id=2,
        latest_transaction_id=20,
        type="07",
        total_loan_value=30,
        action_date="2020-01-01",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="005",
        pop_county_fips="45005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="02",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_fips="45001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="90",
        recipient_location_congressional_code_current="03",
        recipient_name="RECIPIENT 2",
        recipient_unique_id="456789123",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        recipient_levels=["R"],
        disaster_emergency_fund_codes=["L"],
        total_covid_outlay=10,
        total_covid_obligation=20,
        spending_by_defc=[{"defc": "L", "outlay": 10, "obligation": 20}],
    )
    award3 = baker.make(
        "search.AwardSearch",
        award_id=3,
        latest_transaction_id=30,
        type="08",
        total_loan_value=300,
        action_date="2022-01-03",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_state_code="WA",
        pop_county_code="005",
        pop_county_fips="53005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="02",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_fips="45001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="02",
        recipient_name="RECIPIENT, 3",
        recipient_unique_id="987654321",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_levels=["C", "R"],
        disaster_emergency_fund_codes=["L"],
        total_covid_outlay=100,
        total_covid_obligation=200,
        spending_by_defc=[{"defc": "L", "outlay": 100, "obligation": 200}],
    )
    award4 = baker.make(
        "search.AwardSearch",
        award_id=4,
        latest_transaction_id=50,
        type="B",
        total_loan_value=0,
        action_date="2022-01-04",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="WA",
        pop_county_code="005",
        pop_county_fips="45005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="02",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_fips="53005",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="02",
        recipient_name="RECIPIENT, 3",
        recipient_unique_id="987654321",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_levels=["C", "R"],
        disaster_emergency_fund_codes=["L"],
        total_covid_outlay=1000,
        total_covid_obligation=2000,
        spending_by_defc=[{"defc": "L", "outlay": 1000, "obligation": 2000}],
    )
    award5 = baker.make(
        "search.AwardSearch",
        award_id=5,
        latest_transaction_id=40,
        type="A",
        total_loan_value=0,
        action_date="2022-01-05",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_fips="45001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="10",
        pop_congressional_code_current="01",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_fips="53005",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="02",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id="096354360",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        recipient_levels=["R"],
        disaster_emergency_fund_codes=["M"],
        total_covid_outlay=10000,
        total_covid_obligation=20000,
        spending_by_defc=[{"defc": "M", "outlay": 10000, "obligation": 20000}],
    )
    award6 = baker.make(
        "search.AwardSearch",
        award_id=6,
        latest_transaction_id=60,
        type="C",
        total_loan_value=0,
        action_date="2022-01-06",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_fips="45001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="90",
        pop_congressional_code_current="03",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_fips="45005",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="02",
        recipient_name="RECIPIENT, 3",
        recipient_unique_id="987654321",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_levels=["C", "R"],
        disaster_emergency_fund_codes=["M"],
        total_covid_outlay=100000,
        total_covid_obligation=200000,
        spending_by_defc=[{"defc": "M", "outlay": 100000, "obligation": 200000}],
    )
    award7 = baker.make(
        "search.AwardSearch",
        award_id=7,
        latest_transaction_id=70,
        type="D",
        total_loan_value=0,
        action_date="2022-01-07",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_fips="45001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="90",
        pop_congressional_code_current="03",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_fips="45001",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="10",
        recipient_location_congressional_code_current="01",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id=None,
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        recipient_levels=None,
        disaster_emergency_fund_codes=["M"],
        total_covid_outlay=1000000,
        total_covid_obligation=2000000,
        spending_by_defc=[{"defc": "M", "outlay": 1000000, "obligation": 2000000}],
    )

    # Disaster Emergency Fund Code
    defc1 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
    )
    defc2 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="N",
        public_law="PUBLIC LAW FOR CODE N",
        title="TITLE FOR CODE N",
        group_name="covid_19",
    )

    # Submission Attributes
    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )
    sub2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )
    sub3 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        is_final_balances_for_fy=True,
        reporting_period_start="2022-05-01",
        submission_window_id="2022081",
    )

    # Toptier Agency
    ta1 = baker.make(
        "references.ToptierAgency", toptier_agency_id=7, toptier_code="007", name="Agency 007", _fill_optional=True
    )

    # Federal Account
    fa1 = baker.make(
        "accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1", parent_toptier_agency=ta1
    )

    # Treasury Approriation Account
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

    # Financial Accounts by Awards
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=1,
        award=award1,
        submission=sub1,
        disaster_emergency_fund=defc1,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=2,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=2,
        award=award2,
        submission=sub1,
        disaster_emergency_fund=defc1,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=10,
        transaction_obligated_amount=20,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=3,
        award=award3,
        submission=sub2,
        disaster_emergency_fund=defc1,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=100,
        transaction_obligated_amount=200,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=4,
        award=award4,
        submission=sub2,
        disaster_emergency_fund=defc1,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=1000,
        transaction_obligated_amount=2000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=5,
        award=award5,
        submission=sub3,
        disaster_emergency_fund=defc2,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=10000,
        transaction_obligated_amount=20000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=6,
        award=award6,
        submission=sub3,
        disaster_emergency_fund=defc2,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=100000,
        transaction_obligated_amount=200000,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        pk=7,
        award=award7,
        submission=sub3,
        disaster_emergency_fund=defc2,
        treasury_account=tas1,
        gross_outlay_amount_by_award_cpe=1000000,
        transaction_obligated_amount=2000000,
    )

    # DABS Submission Window Schedule
    baker.make(
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
    baker.make(
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

    # Transaction Search
    baker.make(
        "search.TransactionSearch",
        transaction_id=10,
        award=award1,
        federal_action_obligation=5,
        action_date="2022-01-01",
        is_fpds=False,
        cfda_number="10.100",
        pop_country_code="USA",
        pop_state_code=None,
        pop_county_code=None,
        pop_county_name=None,
        pop_congressional_code=None,
        recipient_location_country_code=None,
        recipient_location_state_code=None,
        recipient_location_county_code=None,
        recipient_location_county_name=None,
        recipient_location_congressional_code=None,
        recipient_name="RECIPIENT 1",
        recipient_name_raw="RECIPIENT 1",
        recipient_unique_id=None,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=20,
        award=award2,
        federal_action_obligation=50,
        action_date="2022-01-02",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="90",
        recipient_name="RECIPIENT 2",
        recipient_name_raw="RECIPIENT 2",
        recipient_unique_id="456789123",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=30,
        award=award3,
        federal_action_obligation=500,
        action_date="2022-01-03",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_state_code="WA",
        pop_county_code="005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="50",
        recipient_name="RECIPIENT, 3",
        recipient_name_raw="RECIPIENT, 3",
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=40,
        award=award4,
        federal_action_obligation=5000,
        action_date="2022-01-04",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="WA",
        pop_county_code="005",
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_unique_id="096354360",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=50,
        award=award5,
        federal_action_obligation=50000,
        action_date="2022-01-05",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="10",
        recipient_location_country_code="USA",
        recipient_location_state_code="WA",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=60,
        award=award6,
        federal_action_obligation=500000,
        action_date="2022-01-06",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="90",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="005",
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="987654321",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=70,
        award=award7,
        federal_action_obligation=5000000,
        action_date="2022-01-07",
        is_fpds=True,
        pop_country_code="USA",
        pop_state_code="SC",
        pop_county_code="001",
        pop_county_name="CHARLESTON",
        pop_congressional_code="90",
        recipient_location_country_code="USA",
        recipient_location_state_code="SC",
        recipient_location_county_code="001",
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="10",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_unique_id=None,
    )

    # References State Data
    baker.make("recipient.StateData", id="45-2022", fips="45", code="SC", name="South Carolina")
    baker.make("recipient.StateData", id="53-2022", fips="53", code="WA", name="Washington")

    # References Country
    baker.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")

    # References Population County
    baker.make(
        "references.PopCounty",
        id=1,
        state_code="45",
        state_name="South Carolina",
        county_number="001",
        latest_population=1,
    )
    baker.make(
        "references.PopCounty",
        id=2,
        state_code="45",
        state_name="South Carolina",
        county_number="005",
        latest_population=10,
    )
    baker.make(
        "references.PopCounty",
        id=3,
        state_code="53",
        state_name="Washington",
        county_number="005",
        latest_population=100,
    )
    baker.make(
        "references.PopCounty",
        id=4,
        state_code="45",
        state_name="South Carolina",
        county_number="000",
        latest_population=1000,
    )
    baker.make(
        "references.PopCounty",
        id=5,
        state_code="53",
        state_name="Washington",
        county_number="000",
        latest_population=10000,
    )

    # References Population Congressional District
    baker.make(
        "references.PopCongressionalDistrict",
        id=1,
        state_code="45",
        state_name="South Carolina",
        congressional_district="90",
        latest_population=1,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=2,
        state_code="45",
        state_name="South Carolina",
        congressional_district="10",
        latest_population=10,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=2,
        state_code="45",
        state_name="South Carolina",
        congressional_district="50",
        latest_population=100,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=3,
        state_code="53",
        state_name="Washington",
        congressional_district="50",
        latest_population=1000,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=4,
        state_code="45",
        state_name="South Carolina",
        congressional_district="03",
        latest_population=1,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=5,
        state_code="53",
        state_name="Washington",
        congressional_district="02",
        latest_population=1000,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=6,
        state_code="45",
        state_name="South Carolina",
        congressional_district="01",
        latest_population=10,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=6,
        state_code="45",
        state_name="South Carolina",
        congressional_district="02",
        latest_population=100,
    )
    baker.make("references.CityCountyStateCode", county_numeric="005", state_numeric="53", county_name="Test Name")
    baker.make("references.CityCountyStateCode", county_numeric="005", state_numeric="45", county_name="Test Name")
    baker.make("references.CityCountyStateCode", county_numeric="001", state_numeric="45", county_name="Charleston")

    # Recipient Profile
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 1",
        recipient_level="R",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_unique_id=None,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 2",
        recipient_level="R",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        recipient_unique_id="456789123",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="P",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_unique_id="987654321",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="C",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_unique_id="987654321",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_level="R",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        recipient_unique_id="096354360",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT, 3",
        recipient_level="R",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_unique_id="987654321",
    )

    # Recipient Lookup
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT, 3",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        duns="987654321",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 2",
        recipient_hash="3c92491a-f2cd-ec7d-294b-7daf91511866",
        duns="456789123",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 1",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        duns=None,
        uei=None,
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="5bf6217b-4a70-da67-1351-af6ab2e0a4b3",
        duns="096354360",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        duns=None,
    )
    # Ref Country Code
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    baker.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
