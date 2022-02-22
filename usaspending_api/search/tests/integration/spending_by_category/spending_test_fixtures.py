from model_mommy import mommy
import pytest


@pytest.fixture
def basic_agencies(db):
    _setup_agency(1, [], "Awarding")

    _setup_agency(4, [], "Funding")


@pytest.fixture
def basic_award(db, basic_agencies):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=5,
        action_date="2020-01-01",
    )


@pytest.fixture
def agencies_with_subagencies(db):
    """Create some agencies with more than one subtier to toptier"""
    _setup_agency(3, [5], "Awarding")

    _setup_agency(2, [6], "Funding")


@pytest.fixture
def subagency_award(db, agencies_with_subagencies):
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        awarding_agency_id=1005,
        funding_agency_id=1006,
        federal_action_obligation=10,
        action_date="2020-01-02",
    )


def _setup_agency(id, subtiers, special_name):
    mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=id + 2000,
        name=f"{special_name} Toptier Agency {id}",
        abbreviation=f"TA{id}",
        toptier_code=f"00{id}",
    )
    mommy.make(
        "references.SubtierAgency",
        subtier_agency_id=id + 3000,
        name=f"{special_name} Subtier Agency {id}",
        abbreviation=f"SA{id}",
    )
    mommy.make(
        "references.Agency", id=id + 1000, toptier_agency_id=id + 2000, subtier_agency_id=id + 3000, toptier_flag=True
    )

    for sub_id in subtiers:
        mommy.make(
            "references.SubtierAgency",
            subtier_agency_id=sub_id + 3000,
            name=f"{special_name} Subtier Agency {sub_id}",
            abbreviation=f"SA{sub_id}",
        )
        mommy.make(
            "references.Agency",
            id=sub_id + 1000,
            toptier_agency_id=id + 2000,
            subtier_agency_id=sub_id + 3000,
            toptier_flag=False,
        )


@pytest.fixture
def awards_and_transactions(db):
    # Awards
    mommy.make("awards.Award", id=1, latest_transaction_id=10)
    mommy.make("awards.Award", id=2, latest_transaction_id=20)
    mommy.make("awards.Award", id=3, latest_transaction_id=30)
    mommy.make("awards.Award", id=4, latest_transaction_id=40)
    mommy.make("awards.Award", id=5, latest_transaction_id=50)
    mommy.make("awards.Award", id=6, latest_transaction_id=60)
    mommy.make("awards.Award", id=7, latest_transaction_id=70)

    mommy.make("awards.FinancialAccountsByAwards", pk=1, award_id=1, treasury_account_id=1)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=1, federal_account_id=1)
    mommy.make(
        "accounts.FederalAccount",
        pk=1,
        agency_identifier="012",
        main_account_code="1106",
        account_title="FA 1",
        federal_account_code="012-1106",
    )

    mommy.make("awards.FinancialAccountsByAwards", pk=2, award_id=2, treasury_account_id=2)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=2, federal_account_id=2)
    mommy.make(
        "accounts.FederalAccount",
        pk=2,
        agency_identifier="014",
        main_account_code="5110",
        account_title="FA 2",
        federal_account_code="014-5110",
    )

    mommy.make("awards.FinancialAccountsByAwards", pk=3, award_id=2, treasury_account_id=3)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=3, federal_account_id=3)
    mommy.make(
        "accounts.FederalAccount",
        pk=3,
        agency_identifier="014",
        main_account_code="1036",
        account_title="FA 3",
        federal_account_code="014-1036",
    )

    # Transaction Normalized
    mommy.make(
        "awards.TransactionNormalized",
        id=10,
        award_id=1,
        federal_action_obligation=5,
        action_date="2020-01-01",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=20,
        award_id=2,
        federal_action_obligation=50,
        action_date="2020-01-02",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=30,
        award_id=3,
        federal_action_obligation=500,
        action_date="2020-01-03",
        is_fpds=False,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=40,
        award_id=4,
        federal_action_obligation=5000,
        action_date="2020-01-04",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=50,
        award_id=5,
        federal_action_obligation=50000,
        action_date="2020-01-05",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=60,
        award_id=6,
        federal_action_obligation=500000,
        action_date="2020-01-06",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=70,
        award_id=7,
        federal_action_obligation=5000000,
        action_date="2020-01-07",
        is_fpds=True,
    )

    # Transaction FABS
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=10,
        cfda_number="10.100",
        place_of_perform_country_c="USA",
        place_of_perfor_state_code="SC",
        place_of_perform_county_co="001",
        place_of_perform_county_na="CHARLESTON",
        place_of_performance_congr="10",
        legal_entity_country_code="CAN",
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
        awardee_or_recipient_legal="RECIPIENT 3",
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
        product_or_service_code="1005",
        product_or_service_co_desc="PSC 1",
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
        awardee_or_recipient_uniqu="123456789",
        product_or_service_code="M123",
        product_or_service_co_desc="PSC 2",
        naics="111110",
        naics_description="NAICS 1",
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
        awardee_or_recipient_uniqu="123456789",
        naics="222220",
        naics_description="NAICS 2",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=70,
        place_of_perform_country_c="CAN",
        place_of_performance_state=None,
        place_of_perform_county_co=None,
        place_of_perform_county_na=None,
        place_of_performance_congr=None,
        legal_entity_country_code="USA",
        legal_entity_state_code="SC",
        legal_entity_county_code="01",
        legal_entity_county_name="CHARLESTON",
        legal_entity_congressional="10",
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu=None,
    )

    # References State Data
    mommy.make("recipient.StateData", id="45-2020", fips="45", code="SC", name="South Carolina")
    mommy.make("recipient.StateData", id="53-2020", fips="53", code="WA", name="Washington")

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

    # References CFDA
    mommy.make("references.Cfda", id=100, program_number="10.100", program_title="CFDA 1")
    mommy.make("references.Cfda", id=200, program_number="20.200", program_title="CFDA 2")

    # Recipient Profile
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 1",
        recipient_level="R",
        recipient_hash="5f572ec9-8b49-e5eb-22c7-f6ef316f7689",
        recipient_unique_id=None,
    )
    mommy.make(
        "recipient.RecipientProfile", recipient_name="RECIPIENT 2", recipient_level="R", recipient_unique_id="456789123"
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="P",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        recipient_unique_id="987654321",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_name="RECIPIENT 3",
        recipient_level="C",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
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
        recipient_name=None,
        recipient_level="R",
        recipient_hash="25f9e794-323b-4538-85f5-181f1b624d0b",
        recipient_unique_id="123456789",
    )

    # Recipient Lookup
    mommy.make(
        "recipient.RecipientLookup",
        legal_business_name="RECIPIENT 3",
        recipient_hash="bf05f751-6841-efd6-8f1b-0144163eceae",
        duns="987654321",
    )

    # PSC
    mommy.make("references.PSC", code="1005", description="PSC 1")
    mommy.make("references.PSC", code="M123", description="PSC 2")

    # NAICS
    mommy.make("references.NAICS", code="111110", description="NAICS 1")
    mommy.make("references.NAICS", code="222220", description="NAICS 2")

    # Ref Country Code
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    mommy.make("references.RefCountryCode", country_code="CAN", country_name="CANADA")
