import pytest

from model_mommy import mommy
from usaspending_api.awards.models import Award
from usaspending_api.references.models.ref_program_activity import RefProgramActivity


@pytest.fixture
def spending_by_award_test_data():

    mommy.make(
        "recipient.RecipientLookup",
        id=1001,
        recipient_hash="bb7d6b0b-f890-4cec-a8ae-f777c8f5c3a9",
        legal_business_name="recipient_name_for_award_1001",
        duns="duns_1001",
    )
    mommy.make(
        "recipient.RecipientLookup",
        id=1002,
        recipient_hash="180bddfc-67f0-42d6-8279-a014d1062d65",
        legal_business_name="recipient_name_for_award_1002",
        duns="duns_1002",
    )
    mommy.make(
        "recipient.RecipientLookup",
        id=1003,
        recipient_hash="28aae030-b4b4-4494-8a75-3356208469cf",
        legal_business_name="recipient_name_for_award_1003",
        duns="duns_1003",
    )

    mommy.make(
        "recipient.RecipientProfile",
        id=2001,
        recipient_hash="bb7d6b0b-f890-4cec-a8ae-f777c8f5c3a9",
        recipient_level="R",
        recipient_name="recipient_name_1001",
        recipient_unique_id="duns_1001",
    )
    mommy.make(
        "recipient.RecipientProfile",
        id=2002,
        recipient_hash="180bddfc-67f0-42d6-8279-a014d1062d65",
        recipient_level="R",
        recipient_name="recipient_name_1002",
        recipient_unique_id="duns_1002",
    )
    mommy.make(
        "recipient.RecipientProfile",
        id=2003,
        recipient_hash="28aae030-b4b4-4494-8a75-3356208469cf",
        recipient_level="R",
        recipient_name="recipient_name_1003",
        recipient_unique_id="duns_1003",
    )

    mommy.make(
        "awards.Award",
        id=1,
        type="A",
        category="contract",
        piid="abc111",
        earliest_transaction_id=1,
        latest_transaction_id=2,
        generated_unique_award_id="CONT_AWD_TESTING_1",
        date_signed="2008-01-01",
        description="test test test",
        awarding_agency_id=1,
        funding_agency_id=1,
        total_obligation=999999.00,
    )
    mommy.make(
        "awards.Award",
        id=2,
        type="A",
        category="contract",
        piid="abc222",
        earliest_transaction_id=3,
        latest_transaction_id=3,
        generated_unique_award_id="CONT_AWD_TESTING_2",
        date_signed="2009-01-01",
        total_obligation=9016.00,
    )
    mommy.make(
        "awards.Award",
        id=3,
        type="A",
        category="contract",
        piid="abc333",
        earliest_transaction_id=4,
        latest_transaction_id=6,
        generated_unique_award_id="CONT_AWD_TESTING_3",
        date_signed="2010-01-01",
        total_obligation=500000001.00,
    )
    mommy.make(
        "awards.Award",
        id=4,
        type="02",
        category="grant",
        fain="abc444",
        earliest_transaction_id=7,
        latest_transaction_id=7,
        generated_unique_award_id="ASST_NON_TESTING_4",
        date_signed="2019-01-01",
        total_obligation=12.00,
    )

    # Toptier Agency
    toptier_agency_1 = {"pk": 1, "abbreviation": "TA1", "name": "TOPTIER AGENCY 1", "toptier_code": "ABC"}

    mommy.make("references.ToptierAgency", **toptier_agency_1)

    # TAS
    mommy.make(
        "accounts.FederalAccount", id=1, parent_toptier_agency_id=1, agency_identifier="1", main_account_code="0001"
    )
    mommy.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=1,
        agency_id="097",
        main_account_code="4930",
        federal_account_id=1,
    )
    defc = mommy.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="2022080",
        is_quarter=False,
        period_start_date="2022-05-01",
        period_end_date="2022-05-30",
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=8,
        submission_reveal_date="2020-5-15",
    )
    # Unclosed submisssion window
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id="9999070",
        is_quarter=True,
        period_start_date="9999-04-01",
        period_end_date="9999-04-30",
        submission_fiscal_year=9999,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="9999-4-15",
    )
    sa1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        reporting_period_start="2022-05-01",
        submission_window_id="2022080",
    )
    sa2 = mommy.make(
        "submissions.SubmissionAttributes",
        pk=1,
        reporting_fiscal_period=8,
        reporting_fiscal_year=9999,
        reporting_period_end="9999-06-30",
        reporting_period_start="9999-04-02",
        submission_window_id="9999070",
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        treasury_account_id=1,
        transaction_obligated_amount=100,
        gross_outlay_amount_by_award_cpe=100,
        disaster_emergency_fund=defc,
        submission=sa1,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        treasury_account_id=1,
        transaction_obligated_amount=99,
        gross_outlay_amount_by_award_cpe=99,
        disaster_emergency_fund=defc,
        submission=sa2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=2,
        transaction_obligated_amount=0,
        gross_outlay_amount_by_award_cpe=0,
        disaster_emergency_fund=defc,
        submission=sa2,
    )

    # Subtier Agency
    subtier_agency_1 = {"pk": 1, "abbreviation": "SA1", "name": "SUBTIER AGENCY 1", "subtier_code": "DEF"}
    subtier_agency_2 = {"pk": 2, "abbreviation": "SA2", "name": "SUBTIER AGENCY 2", "subtier_code": "1000"}

    mommy.make("references.SubtierAgency", **subtier_agency_1)
    mommy.make("references.SubtierAgency", **subtier_agency_2)

    # Agency
    mommy.make("references.Agency", pk=1, toptier_agency_id=1, subtier_agency_id=1)

    mommy.make("awards.TransactionNormalized", id=1, award_id=1, action_date="2020-04-01", is_fpds=True)
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=1,
        action_date="2020-04-01",
        is_fpds=True,
        business_categories=["business_category_1_3"],
    )
    mommy.make("awards.TransactionNormalized", id=3, award_id=2, action_date="2016-01-01", is_fpds=True)
    mommy.make("awards.TransactionNormalized", id=4, award_id=3, action_date="2017-01-01", is_fpds=True)
    mommy.make("awards.TransactionNormalized", id=5, award_id=3, action_date="2018-01-01", is_fpds=True)
    mommy.make(
        "awards.TransactionNormalized",
        id=6,
        award_id=3,
        action_date="2019-01-01",
        is_fpds=True,
        business_categories=["business_category_2_8"],
    )
    mommy.make("awards.TransactionNormalized", id=7, award_id=4, action_date="2019-10-1", is_fpds=False)
    mommy.make("awards.TransactionFPDS", transaction_id=1)
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        place_of_performance_state="VA",
        place_of_perform_country_c="USA",
        place_of_perform_county_co="013",
        place_of_perform_city_name="ARLINGTON",
        legal_entity_state_code="VA",
        legal_entity_country_code="USA",
        legal_entity_county_code="013",
        legal_entity_city_name="ARLINGTON",
        naics="112233",
        product_or_service_code="PSC1",
        type_of_contract_pricing="contract_pricing_test",
        type_set_aside="type_set_aside_test",
        extent_competed="extent_competed_test",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        place_of_performance_state="VA",
        place_of_perform_country_c="USA",
        place_of_perform_county_co="012",
        legal_entity_state_code="VA",
        legal_entity_country_code="USA",
        legal_entity_county_code="012",
        naics="112244",
    )
    mommy.make("awards.TransactionFPDS", transaction_id=4)
    mommy.make("awards.TransactionFPDS", transaction_id=5)
    mommy.make("awards.TransactionFPDS", transaction_id=6)

    mommy.make("awards.TransactionFABS", transaction_id=7, cfda_number="10.331", awardee_or_recipient_uniqu="duns_1001")

    mommy.make("awards.BrokerSubaward", id=1, award_id=1, subaward_number=11111, awardee_or_recipient_uniqu="duns_1001")
    mommy.make("awards.BrokerSubaward", id=2, award_id=2, subaward_number=22222, awardee_or_recipient_uniqu="duns_1002")
    mommy.make("awards.BrokerSubaward", id=3, award_id=2, subaward_number=33333, awardee_or_recipient_uniqu="duns_1002")
    mommy.make("awards.BrokerSubaward", id=4, award_id=3, subaward_number=44444, awardee_or_recipient_uniqu="duns_1003")
    mommy.make("awards.BrokerSubaward", id=6, award_id=3, subaward_number=66666, awardee_or_recipient_uniqu="duns_1003")

    mommy.make(
        "awards.Subaward",
        id=1,
        award_id=1,
        latest_transaction_id=1,
        subaward_number=11111,
        prime_award_type="A",
        award_type="procurement",
        action_date="2020-04-02",
        amount=10000,
        prime_recipient_name="recipient_name_for_award_1001",
        recipient_unique_id="duns_1001",
        piid="PIID1001",
        awarding_toptier_agency_name="awarding toptier 8001",
        awarding_subtier_agency_name="awarding subtier 8001",
        product_or_service_code="PSC2",
    )
    mommy.make(
        "awards.Subaward",
        id=2,
        award_id=1,
        latest_transaction_id=2,
        subaward_number=22222,
        prime_award_type="A",
        award_type="procurement",
        action_date="2020-04-02",
        amount=20000,
        prime_recipient_name="recipient_name_for_award_1001",
        recipient_unique_id="duns_1001",
        piid="PIID2001",
        awarding_toptier_agency_name="awarding toptier 8002",
        awarding_subtier_agency_name="awarding subtier 8002",
    )
    mommy.make(
        "awards.Subaward",
        id=3,
        award_id=2,
        latest_transaction_id=3,
        subaward_number=33333,
        prime_award_type="A",
        award_type="procurement",
        action_date="2016-01-01",
        amount=30000,
        prime_recipient_name="recipient_name_for_award_1002",
        recipient_unique_id="duns_1002",
        piid="PIID3002",
        awarding_toptier_agency_name="awarding toptier 8003",
        awarding_subtier_agency_name="awarding subtier 8003",
    )
    mommy.make(
        "awards.Subaward",
        id=6,
        award_id=3,
        latest_transaction_id=6,
        subaward_number=66666,
        prime_award_type="A",
        award_type="procurement",
        action_date="2019-01-01",
        amount=60000,
        prime_recipient_name="recipient_name_for_award_1003",
        recipient_unique_id="duns_1003",
        piid="PIID6003",
        awarding_toptier_agency_name="awarding toptier 8006",
        awarding_subtier_agency_name="awarding subtier 8006",
    )

    # Ref Program Activity
    ref_program_activity_1 = {"id": 1}
    mommy.make("references.RefProgramActivity", **ref_program_activity_1)

    # Ref Object Class
    ref_object_class_1 = {"id": 1, "object_class": "111"}
    mommy.make("references.ObjectClass", **ref_object_class_1)

    # Financial Accounts by Awards
    financial_accounts_by_awards_1 = {
        "award": Award.objects.get(pk=1),
        "program_activity": RefProgramActivity.objects.get(pk=1),
    }
    mommy.make("awards.FinancialAccountsByAwards", **financial_accounts_by_awards_1)

    # Ref Country Code
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
