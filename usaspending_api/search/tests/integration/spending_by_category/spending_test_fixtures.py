import pytest
from model_bakery import baker


@pytest.fixture
def basic_agencies(db):
    _setup_agency(1, [], "Awarding")

    _setup_agency(4, [], "Funding")


@pytest.fixture
def basic_award(db, basic_agencies):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        federal_action_obligation=5,
        generated_pragmatic_obligation=5,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        is_fpds=False,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        awarding_toptier_agency_id=1001,
        funding_toptier_agency_id=1004,
        awarding_agency_code="001",
        awarding_toptier_agency_name="Awarding Toptier Agency 1",
        awarding_toptier_agency_abbreviation="TA1",
        funding_agency_code="004",
        funding_toptier_agency_name="Funding Toptier Agency 4",
        funding_toptier_agency_abbreviation="TA4",
        awarding_sub_tier_agency_c="1001",
        awarding_subtier_agency_name="Awarding Subtier Agency 1",
        awarding_subtier_agency_abbreviation="SA1",
        funding_sub_tier_agency_co="1004",
        funding_subtier_agency_name="Funding Subtier Agency 4",
        funding_subtier_agency_abbreviation="SA4",
    )


@pytest.fixture
def agencies_with_subagencies(db):
    """Create some agencies with more than one subtier to toptier"""
    _setup_agency(3, [5], "Awarding")

    _setup_agency(2, [6], "Funding")


@pytest.fixture
def subagency_award(db, agencies_with_subagencies):
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)

    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        awarding_agency_code="003",
        awarding_toptier_agency_name="Awarding Toptier Agency 3",
        awarding_toptier_agency_abbreviation="TA3",
        funding_agency_code="002",
        funding_toptier_agency_name="Funding Toptier Agency 2",
        funding_toptier_agency_abbreviation="TA2",
        awarding_sub_tier_agency_c="1005",
        awarding_subtier_agency_name="Awarding Subtier Agency 5",
        awarding_subtier_agency_abbreviation="SA5",
        funding_sub_tier_agency_co="1006",
        funding_subtier_agency_name="Funding Subtier Agency 6",
        funding_subtier_agency_abbreviation="SA6",
        federal_action_obligation=10,
        generated_pragmatic_obligation=10,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        award_date_signed="2020-01-14",
        is_fpds=False,
        awarding_agency_id=1003,
        funding_agency_id=1002,
        awarding_toptier_agency_id=1003,
        funding_toptier_agency_id=1002,
    )


def _setup_agency(id, subtiers, special_name):
    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=id + 2000,
        name=f"{special_name} Toptier Agency {id}",
        abbreviation=f"TA{id}",
        toptier_code=f"00{id}",
    )
    baker.make(
        "references.SubtierAgency",
        subtier_agency_id=id + 3000,
        name=f"{special_name} Subtier Agency {id}",
        abbreviation=f"SA{id}",
        subtier_code=f"100{id}",
    )
    baker.make(
        "references.Agency", id=id + 1000, toptier_agency_id=id + 2000, subtier_agency_id=id + 3000, toptier_flag=True
    )

    for sub_id in subtiers:
        baker.make(
            "references.SubtierAgency",
            subtier_agency_id=sub_id + 3000,
            name=f"{special_name} Subtier Agency {sub_id}",
            abbreviation=f"SA{sub_id}",
            subtier_code=f"100{sub_id}",
        )
        baker.make(
            "references.Agency",
            id=sub_id + 1000,
            toptier_agency_id=id + 2000,
            subtier_agency_id=sub_id + 3000,
            toptier_flag=False,
        )


@pytest.fixture
def awards_and_transactions(db):
    # Awards
    award1 = baker.make("search.AwardSearch", award_id=1, latest_transaction_id=10, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=20, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=30, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=40, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=5, latest_transaction_id=50, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=6, latest_transaction_id=60, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=7, latest_transaction_id=70, action_date="2020-01-01")
    baker.make("search.AwardSearch", award_id=8, latest_transaction_id=80, action_date="2020-01-01")
    ref_program_activity1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code=123,
        program_activity_name="PROGRAM_ACTIVITY_123",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award_id=award1.award_id,
        treasury_account_id=1,
        program_activity_id=ref_program_activity1.id,
    )
    baker.make("accounts.TreasuryAppropriationAccount", pk=1, federal_account_id=1)
    baker.make(
        "accounts.FederalAccount",
        pk=1,
        agency_identifier="012",
        main_account_code="1106",
        account_title="FA 1",
        federal_account_code="012-1106",
    )
    fa1 = {"id": 1, "account_title": "FA 1", "federal_account_code": "012-1106"}

    baker.make("awards.FinancialAccountsByAwards", pk=2, award_id=2, treasury_account_id=2)
    baker.make("accounts.TreasuryAppropriationAccount", pk=2, federal_account_id=2)
    baker.make(
        "accounts.FederalAccount",
        pk=2,
        agency_identifier="014",
        main_account_code="5110",
        account_title="FA 2",
        federal_account_code="014-5110",
    )
    fa2 = {"id": 2, "account_title": "FA 2", "federal_account_code": "014-5110"}

    baker.make("awards.FinancialAccountsByAwards", pk=3, award_id=2, treasury_account_id=3)
    baker.make("accounts.TreasuryAppropriationAccount", pk=3, federal_account_id=3)
    baker.make(
        "accounts.FederalAccount",
        pk=3,
        agency_identifier="014",
        main_account_code="1036",
        account_title="FA 3",
        federal_account_code="014-1036",
    )
    fa3 = {"id": 3, "account_title": "FA 3", "federal_account_code": "014-1036"}

    # Transaction Search
    baker.make(
        "search.TransactionSearch",
        transaction_id=10,
        award_id=1,
        federal_action_obligation=5,
        generated_pragmatic_obligation=5,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        award_date_signed="2020-01-14",
        is_fpds=False,
        cfda_number="10.100",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="SC",
        pop_state_fips="45",
        pop_state_population=1000,
        pop_state_name="SOUTH CAROLINA",
        pop_county_code="001",
        pop_county_population=1,
        pop_county_name="CHARLESTON",
        pop_congressional_code="10",
        pop_congressional_code_current="10",
        pop_congressional_population=None,
        recipient_location_country_code="CAN",
        recipient_location_country_name="CANADA",
        recipient_location_state_code=None,
        recipient_location_state_fips=None,
        recipient_location_state_population=None,
        recipient_location_state_name=None,
        recipient_location_county_code=None,
        recipient_location_county_population=None,
        recipient_location_county_name=None,
        recipient_location_congressional_code=None,
        recipient_location_congressional_population=None,
        recipient_name="RECIPIENT 1",
        recipient_name_raw="RECIPIENT 1",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_levels=["R"],
        recipient_unique_id=None,
        recipient_uei=None,
        federal_accounts=[fa1],
        award_category="grant",
        fiscal_year="2020",
        program_activities=[
            {
                "code": str(ref_program_activity1.program_activity_code).zfill(4),
                "name": ref_program_activity1.program_activity_name,
            }
        ],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=20,
        award_id=2,
        federal_action_obligation=50,
        generated_pragmatic_obligation=50,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="SC",
        pop_state_fips="45",
        pop_state_population=1000,
        pop_state_name="SOUTH CAROLINA",
        pop_county_code="005",
        pop_county_population=10,
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="51",
        pop_congressional_population=100,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_fips="45",
        recipient_location_state_population=1000,
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="001",
        recipient_location_county_population=1,
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="90",
        recipient_location_congressional_code_current="90",
        recipient_location_congressional_population=1,
        recipient_name="RECIPIENT 2",
        recipient_name_raw="RECIPIENT 2",
        recipient_hash="7976667a-dd95-2b65-5f4e-e340c686a346",
        recipient_levels=["R"],
        recipient_unique_id="456789123",
        recipient_uei="UEIAAABBBCCC",
        federal_accounts=[fa2, fa3],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=30,
        award_id=3,
        federal_action_obligation=500,
        generated_pragmatic_obligation=500,
        action_date="2020-01-03",
        fiscal_action_date="2020-04-03",
        is_fpds=False,
        cfda_number="20.200",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="WA",
        pop_state_fips="53",
        pop_state_population=10000,
        pop_state_name="WASHINGTON",
        pop_county_code="005",
        pop_county_population=100,
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="51",
        pop_congressional_population=1000,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_fips="45",
        recipient_location_state_population=1000,
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="001",
        recipient_location_county_population=1,
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="51",
        recipient_location_congressional_population=100,
        recipient_name="RECIPIENT 3",
        recipient_name_raw="RECIPIENT 3",
        recipient_hash="3523fd0b-c1f0-ddac-e217-7b7b25fad06f",
        recipient_levels=["C"],
        recipient_unique_id="987654321",
        recipient_uei="987654321AAA",
        federal_accounts=[],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=40,
        award_id=4,
        federal_action_obligation=5000,
        generated_pragmatic_obligation=5000,
        action_date="2020-01-04",
        fiscal_action_date="2020-04-04",
        is_fpds=True,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="WA",
        pop_state_fips="53",
        pop_state_population=10000,
        pop_state_name="WASHINGTON",
        pop_county_code="005",
        pop_county_population=100,
        pop_county_name="TEST NAME",
        pop_congressional_code="50",
        pop_congressional_code_current="51",
        pop_congressional_population=1000,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="WA",
        recipient_location_state_fips="53",
        recipient_location_state_population=10000,
        recipient_location_state_name="WASHINGTON",
        recipient_location_county_code="005",
        recipient_location_county_population=100,
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="51",
        recipient_location_congressional_population=1000,
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_hash="b1bcf17e-d0dc-d9ad-866c-ca262cb05029",
        recipient_levels=["R"],
        recipient_unique_id="096354360",
        recipient_uei="096354360AAA",
        parent_uei="096354360AAA",
        product_or_service_code="1005",
        product_or_service_description="PSC 1",
        federal_accounts=[],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=50,
        award_id=5,
        federal_action_obligation=50000,
        generated_pragmatic_obligation=50000,
        action_date="2020-01-05",
        fiscal_action_date="2020-04-05",
        is_fpds=True,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="SC",
        pop_state_fips="45",
        pop_state_population=1000,
        pop_state_name="SOUTH CAROLINA",
        pop_county_code="001",
        pop_county_population=1,
        pop_county_name="CHARLESTON",
        pop_congressional_code="10",
        pop_congressional_code_current="10",
        pop_congressional_population=None,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="WA",
        recipient_location_state_fips="53",
        recipient_location_state_population=10000,
        recipient_location_state_name="WASHINGTON",
        recipient_location_county_code="005",
        recipient_location_county_population=100,
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="51",
        recipient_location_congressional_population=1000,
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="123456789",
        recipient_hash="f1400310-181e-9a06-ac94-0d80a819bb5e",
        recipient_levels=["R"],
        recipient_uei="123456789AAA",
        parent_uei="123456789AAA",
        product_or_service_code="M123",
        product_or_service_description="PSC 2",
        naics_code="111110",
        naics_description="NAICS 1",
        federal_accounts=[],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=60,
        award_id=6,
        federal_action_obligation=500000,
        generated_pragmatic_obligation=500000,
        action_date="2020-01-06",
        fiscal_action_date="2020-04-06",
        is_fpds=True,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="SC",
        pop_state_fips="45",
        pop_state_population=1000,
        pop_state_name="SOUTH CAROLINA",
        pop_county_code="001",
        pop_county_population=1,
        pop_county_name="CHARLESTON",
        pop_congressional_code="90",
        pop_congressional_code_current="90",
        pop_congressional_population=1,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_fips="45",
        recipient_location_state_population=1000,
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="005",
        recipient_location_county_population=10,
        recipient_location_county_name="TEST NAME",
        recipient_location_congressional_code="50",
        recipient_location_congressional_code_current="51",
        recipient_location_congressional_population=100,
        recipient_name=None,
        recipient_name_raw=None,
        recipient_unique_id="123456789",
        recipient_hash="f1400310-181e-9a06-ac94-0d80a819bb5e",
        recipient_levels=["R"],
        recipient_uei="123456789AAA",
        parent_uei="123456789AAA",
        naics_code="222220",
        naics_description="NAICS 2",
        federal_accounts=[],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=70,
        award_id=7,
        federal_action_obligation=5000000,
        generated_pragmatic_obligation=5000000,
        action_date="2020-01-07",
        fiscal_action_date="2020-04-07",
        is_fpds=True,
        pop_country_code="CAN",
        pop_country_name="CANADA",
        pop_state_code=None,
        pop_state_fips=None,
        pop_state_population=None,
        pop_state_name=None,
        pop_county_code=None,
        pop_county_population=None,
        pop_county_name=None,
        pop_congressional_code=None,
        pop_congressional_population=None,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="SC",
        recipient_location_state_fips="45",
        recipient_location_state_population=1000,
        recipient_location_state_name="SOUTH CAROLINA",
        recipient_location_county_code="001",
        recipient_location_county_population=1,
        recipient_location_county_name="CHARLESTON",
        recipient_location_congressional_code="10",
        recipient_location_congressional_code_current="11",
        recipient_location_congressional_population=None,
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        recipient_levels=["R"],
        recipient_unique_id=None,
        parent_uei=None,
        federal_accounts=[],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=80,
        award_id=7,
        federal_action_obligation=5000000,
        generated_pragmatic_obligation=5000000,
        action_date="2020-01-07",
        fiscal_action_date="2020-04-07",
        is_fpds=True,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        recipient_location_country_code="JPN",
        recipient_location_country_name="JAPAN",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_name_raw="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
    )

    # Transactions with state codes, but no country code
    baker.make(
        "search.TransactionSearch",
        transaction_id=998,
        federal_action_obligation=10,
        generated_pragmatic_obligation=10,
        action_date="2020-01-07",
        pop_state_code="ND",
        pop_state_name="NORTH DAKOTA",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=999,
        federal_action_obligation=20,
        generated_pragmatic_obligation=20,
        action_date="2020-01-07",
        pop_state_code="TX",
        pop_state_name="TEXAS",
    )

    # Subawards
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=7,
        subaward_amount=12345,
        sub_place_of_perform_country_co="CAN",
        sub_legal_entity_country_code="USA",
        sub_action_date="2019-01-07",
        action_date="2019-01-07",
        sub_fiscal_year=2019,
        subaward_type="sub-contract",
        program_activities=[{"name": "PROGRAM_1", "code": "0001"}],
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award_id=8,
        subaward_amount=678910,
        sub_place_of_perform_country_co="USA",
        sub_legal_entity_country_code="JPN",
        sub_action_date="2020-01-07",
        action_date="2020-01-07",
        sub_fiscal_year=2020,
        subaward_type="sub-contract",
        program_activities=[{"name": "PROGRAM_2", "code": "0002"}],
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        award_id=1,
        subaward_amount=54321,
        sub_place_of_perform_country_co="USA",
        sub_legal_entity_country_code="USA",
        sub_action_date="2020-01-07",
        action_date="2020-01-07",
        prime_award_group="grant",
        sub_fiscal_year=2020,
        subaward_type="sub-grant",
        program_activities=[{"name": "PROGRAM_ACTIVITY_123", "code": "0003"}],
    )

    # References State Data
    baker.make("recipient.StateData", id="45-2020", fips="45", code="SC", name="South Carolina")
    baker.make("recipient.StateData", id="53-2020", fips="53", code="WA", name="Washington")

    # Ref Country Code
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    baker.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
    baker.make("references.RefCountryCode", country_code="JPN", country_name="JAPAN")

    # References Population County
    baker.make(
        "references.PopCounty",
        id=1,
        state_code="45",
        state_name="South Carolina",
        county_number="001",
        county_name="Charleston",
        latest_population=1,
    )
    baker.make(
        "references.PopCounty",
        id=2,
        state_code="45",
        state_name="South Carolina",
        county_number="005",
        county_name="Test Name",
        latest_population=10,
    )
    baker.make(
        "references.PopCounty",
        id=3,
        state_code="53",
        state_name="Washington",
        county_number="005",
        county_name="Test Name",
        latest_population=100,
    )
    baker.make(
        "references.PopCounty",
        id=4,
        state_code="45",
        state_name="South Carolina",
        county_number="000",
        county_name="South Carolina",
        latest_population=1000,
    )
    baker.make(
        "references.PopCounty",
        id=5,
        state_code="53",
        state_name="Washington",
        county_number="000",
        county_name="Washington",
        latest_population=10000,
    )
    baker.make(
        "references.PopCounty",
        id=6,
        state_code="38",
        state_name="North Dakota",
        county_number="000",
        latest_population=10,
    )
    baker.make(
        "references.PopCounty",
        id=7,
        state_code="48",
        state_name="Texas",
        county_number="000",
        latest_population=100,
    )

    # References Population Congressional District
    baker.make(
        "references.PopCongressionalDistrict",
        id=2,
        state_code="45",
        state_name="South Carolina",
        state_abbreviation="SC",
        congressional_district="10",
        latest_population=10,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=3,
        state_code="45",
        state_name="South Carolina",
        state_abbreviation="SC",
        congressional_district="50",
        latest_population=100,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=5,
        state_code="45",
        state_name="South Carolina",
        state_abbreviation="SC",
        congressional_district="51",
        latest_population=100,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=4,
        state_code="53",
        state_name="Washington",
        state_abbreviation="WA",
        congressional_district="50",
        latest_population=1000,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=6,
        state_code="53",
        state_name="Washington",
        state_abbreviation="WA",
        congressional_district="51",
        latest_population=2000,
    )
    baker.make(
        "references.PopCongressionalDistrict",
        id=7,
        state_code="45",
        state_name="South Carolina",
        state_abbreviation="SC",
        congressional_district="11",
        latest_population=10,
    )

    # References CFDA
    baker.make("references.Cfda", id=100, program_number="10.100", program_title="CFDA 1")
    baker.make("references.Cfda", id=200, program_number="20.200", program_title="CFDA 2")

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
        recipient_unique_id="456789123",
        recipient_hash="7976667a-dd95-2b65-5f4e-e340c686a346",
        uei="UEIAAABBBCCC",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="P",
        recipient_hash="3523fd0b-c1f0-ddac-e217-7b7b25fad06f",
        recipient_unique_id="987654321",
        uei="987654321AAA",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="C",
        recipient_hash="3523fd0b-c1f0-ddac-e217-7b7b25fad06f",
        recipient_unique_id="987654321",
        uei="987654321AAA",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_level="R",
        recipient_hash="b1bcf17e-d0dc-d9ad-866c-ca262cb05029",
        recipient_unique_id="096354360",
        uei="096354360AAA",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_name=None,
        recipient_level="R",
        recipient_hash="f1400310-181e-9a06-ac94-0d80a819bb5e",
        recipient_unique_id="123456789",
        uei="123456789AAA",
    )

    # Recipient Lookup
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 1",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        duns=None,
        uei=None,
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 2",
        recipient_hash="7976667a-dd95-2b65-5f4e-e340c686a346",
        duns="456789123",
        uei="UEIAAABBBCCC",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 3",
        recipient_hash="3523fd0b-c1f0-ddac-e217-7b7b25fad06f",
        duns="987654321",
        uei="987654321AAA",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="b1bcf17e-d0dc-d9ad-866c-ca262cb05029",
        duns="096354360",
        uei="096354360AAA",
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        duns=None,
        uei=None,
    )
    baker.make(
        "recipient.RecipientLookup",
        legal_business_name=None,
        recipient_hash="f1400310-181e-9a06-ac94-0d80a819bb5e",
        duns="123456789",
        uei="123456789AAA",
    )

    # PSC
    baker.make("references.PSC", code="1005", description="PSC 1")
    baker.make("references.PSC", code="M123", description="PSC 2")

    # NAICS
    baker.make("references.NAICS", code="111110", description="NAICS 1")
    baker.make("references.NAICS", code="222220", description="NAICS 2")
