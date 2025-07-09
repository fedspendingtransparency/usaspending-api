import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, state_to_code
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def psc_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3)
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4)

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        is_fpds=True,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        product_or_service_code="1234",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        is_fpds=True,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        product_or_service_code="1234",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        is_fpds=True,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-03",
        fiscal_action_date="2020-04-03",
        product_or_service_code="9876",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        is_fpds=True,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-04",
        fiscal_action_date="2020-04-04",
        product_or_service_code="9876",
    )

    baker.make("references.PSC", code="1234", description="PSC DESCRIPTION UP")
    baker.make("references.PSC", code="9876", description="PSC DESCRIPTION DOWN")


@pytest.fixture
def cfda_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=1,
        subaward_amount=1,
        cfda_id=1,
        cfda_numbers="CFDA1234",
        cfda_title="CFDA TITLE 1234",
        action_date="2020-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award_id=2,
        subaward_amount=1,
        cfda_id=1,
        cfda_numbers="CFDA1234",
        cfda_title="CFDA TITLE 1234",
        action_date="2020-01-02",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        is_fpds=False,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        cfda_number="CFDA1234",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        is_fpds=False,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        cfda_number="CFDA1234",
    )

    baker.make("references.Cfda", id=1, program_number="CFDA1234", program_title="CFDA TITLE 1234")


@pytest.fixture
def naics_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3)
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4)

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        is_fpds=True,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        naics_code="NAICS 1234",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        is_fpds=True,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        naics_code="NAICS 1234",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        is_fpds=True,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-03",
        fiscal_action_date="2020-04-03",
        naics_code="NAICS 9876",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        is_fpds=True,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-04",
        fiscal_action_date="2020-04-04",
        naics_code="NAICS 9876",
    )

    baker.make("references.NAICS", code="NAICS 1234", description="SOURCE NAICS DESC 1234", year=1955)
    baker.make("references.NAICS", code="NAICS 9876", description="SOURCE NAICS DESC 9876", year=1985)


@pytest.fixture
def agency_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        latest_transaction_id=1,
        subaward_amount=50,
        awarding_agency_id=1003,
        funding_agency_id=1004,
        awarding_toptier_agency_name="Awarding Toptier Agency 3",
        awarding_subtier_agency_name="Awarding Subtier Agency 3",
        funding_toptier_agency_name="Funding Toptier Agency 4",
        funding_subtier_agency_name="Funding Subtier Agency 4",
        awarding_toptier_agency_abbreviation="TA3",
        awarding_subtier_agency_abbreviation="SA3",
        funding_toptier_agency_abbreviation="TA4",
        funding_subtier_agency_abbreviation="SA4",
        action_date="2020-01-01",
        awarding_sub_tier_agency_c="SA3",
        funding_sub_tier_agency_co="SA4",
        funding_toptier_agency_code="TA4",
        awarding_toptier_agency_code="TA3",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        latest_transaction_id=2,
        subaward_amount=100,
        awarding_agency_id=1003,
        funding_agency_id=1004,
        awarding_toptier_agency_name="Awarding Toptier Agency 3",
        awarding_subtier_agency_name="Awarding Subtier Agency 3",
        funding_toptier_agency_name="Funding Toptier Agency 4",
        funding_subtier_agency_name="Funding Subtier Agency 4",
        awarding_toptier_agency_abbreviation="TA3",
        awarding_subtier_agency_abbreviation="SA3",
        funding_toptier_agency_abbreviation="TA4",
        funding_subtier_agency_abbreviation="SA4",
        action_date="2020-01-02",
        awarding_sub_tier_agency_c="SA3",
        funding_sub_tier_agency_co="SA4",
        funding_toptier_agency_code="TA4",
        awarding_toptier_agency_code="TA3",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        awarding_agency_id=1001,
        awarding_toptier_agency_id=1001,
        funding_agency_id=1002,
        funding_toptier_agency_id=1002,
        federal_action_obligation=5,
        generated_pragmatic_obligation=5,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        is_fpds=False,
        awarding_agency_code="TA1",
        funding_agency_code="TA2",
        awarding_sub_tier_agency_c="SA1",
        funding_sub_tier_agency_co="SA2",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        awarding_agency_id=1001,
        awarding_toptier_agency_id=1001,
        funding_agency_id=1002,
        funding_toptier_agency_id=1002,
        federal_action_obligation=10,
        generated_pragmatic_obligation=10,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        is_fpds=False,
        awarding_agency_code="TA1",
        funding_agency_code="TA2",
        awarding_sub_tier_agency_c="SA1",
        funding_sub_tier_agency_co="SA2",
    )

    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2001,
        name="Awarding Toptier Agency 1",
        abbreviation="TA1",
        toptier_code="TA1",
    )
    baker.make(
        "references.SubtierAgency",
        subtier_agency_id=3001,
        name="Awarding Subtier Agency 1",
        abbreviation="SA1",
        subtier_code="SA1",
    )
    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2003,
        name="Awarding Toptier Agency 3",
        abbreviation="TA3",
        toptier_code="TA3",
    )
    baker.make(
        "references.SubtierAgency",
        subtier_agency_id=3003,
        name="Awarding Subtier Agency 3",
        abbreviation="SA3",
        subtier_code="SA3",
    )

    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2002,
        name="Funding Toptier Agency 2",
        abbreviation="TA2",
        toptier_code="TA2",
    )
    baker.make(
        "references.SubtierAgency",
        subtier_agency_id=3002,
        name="Funding Subtier Agency 2",
        abbreviation="SA2",
        subtier_code="SA2",
    )
    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=2004,
        name="Funding Toptier Agency 4",
        abbreviation="TA4",
        toptier_code="TA4",
    )
    baker.make(
        "references.SubtierAgency",
        subtier_agency_id=3004,
        name="Funding Subtier Agency 4",
        abbreviation="SA4",
        subtier_code="SA4",
    )

    baker.make(
        "references.Agency",
        id=1001,
        toptier_agency_id=2001,
        subtier_agency_id=3001,
        toptier_flag=True,
        _fill_optional=True,
    )
    baker.make(
        "references.Agency",
        id=1002,
        toptier_agency_id=2002,
        subtier_agency_id=3002,
        toptier_flag=True,
        _fill_optional=True,
    )

    baker.make(
        "references.Agency",
        id=1003,
        toptier_agency_id=2003,
        subtier_agency_id=3003,
        toptier_flag=True,
        _fill_optional=True,
    )
    baker.make(
        "references.Agency",
        id=1004,
        toptier_agency_id=2004,
        subtier_agency_id=3004,
        toptier_flag=True,
        _fill_optional=True,
    )


@pytest.fixture
def recipient_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3)
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4)
    baker.make("search.AwardSearch", award_id=5, latest_transaction_id=5)

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=1,
        subaward_amount=1,
        sub_awardee_or_recipient_legal_raw="UNIVERSITY OF PAWNEE",
        sub_awardee_or_recipient_legal="UNIVERSITY OF PAWNEE",
        sub_awardee_or_recipient_uniqu="00UOP00",
        subaward_recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        subaward_recipient_level="P",
        action_date="2020-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award_id=2,
        subaward_amount=10,
        sub_awardee_or_recipient_legal_raw="UNIVERSITY OF PAWNEE",
        sub_awardee_or_recipient_legal="UNIVERSITY OF PAWNEE",
        sub_awardee_or_recipient_uniqu="00UOP00",
        subaward_recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        subaward_recipient_level="P",
        action_date="2020-01-02",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        award_id=3,
        subaward_amount=100,
        sub_awardee_or_recipient_legal_raw="JOHN DOE",
        sub_awardee_or_recipient_legal="JOHN DOE",
        sub_awardee_or_recipient_uniqu="1234JD4321",
        subaward_recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        subaward_recipient_level="C",
        action_date="2020-02-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        award_id=4,
        subaward_amount=1000,
        sub_awardee_or_recipient_legal_raw="JOHN DOE",
        sub_awardee_or_recipient_legal="JOHN DOE",
        sub_awardee_or_recipient_uniqu="1234JD4321",
        subaward_recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        subaward_recipient_level="C",
        action_date="2020-02-02",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=5,
        award_id=5,
        subaward_amount=10000,
        sub_awardee_or_recipient_legal_raw="MULTIPLE RECIPIENTS",
        sub_awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        sub_awardee_or_recipient_uniqu=None,
        subaward_recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        action_date="2020-03-01",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        is_fpds=True,
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        recipient_levels=["P"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        is_fpds=True,
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        recipient_levels=["P"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-03",
        fiscal_action_date="2020-04-03",
        is_fpds=True,
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        recipient_levels=["C"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        federal_action_obligation=10,
        generated_pragmatic_obligation=10,
        action_date="2020-01-04",
        fiscal_action_date="2020-04-04",
        is_fpds=True,
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        recipient_levels=["C"],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        award_id=5,
        federal_action_obligation=15,
        generated_pragmatic_obligation=15,
        action_date="2020-01-05",
        fiscal_action_date="2020-04-05",
        is_fpds=True,
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        recipient_levels=[],
    )

    baker.make(
        "recipient.RecipientLookup",
        duns="00UOP00",
        legal_business_name="UNIVERSITY OF PAWNEE",
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
    )
    baker.make(
        "recipient.RecipientLookup",
        duns="1234JD4321",
        legal_business_name="JOHN DOE",
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
    )
    baker.make(
        "recipient.RecipientLookup",
        duns=None,
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
    )

    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="00UOP00",
        recipient_level="P",
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        recipient_name="UNIVERSITY OF PAWNEE",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="1234JD4321",
        recipient_level="C",
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        recipient_name="JOHN DOE",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id=None,
        recipient_level="R",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        recipient_name="MULTIPLE RECIPIENTS",
    )


@pytest.fixture
def geo_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3)
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4)

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=1,
        subaward_amount=1,
        sub_place_of_perform_country_na=None,
        sub_place_of_perform_country_co_raw="US",
        sub_place_of_perform_country_co="US",
        sub_place_of_perform_state_code="XY",
        sub_place_of_perform_county_code="004",
        sub_place_of_perform_county_name="COUNTYSVILLE",
        sub_place_of_performance_zip="12345",
        sub_place_of_perform_congressio_raw="06",
        sub_place_of_perform_congressio="06",
        sub_place_of_performance_congressional_current="90",
        action_date="2020-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award_id=2,
        subaward_amount=10,
        sub_place_of_perform_country_na=None,
        sub_place_of_perform_country_co_raw="US",
        sub_place_of_perform_country_co="US",
        sub_place_of_perform_state_code="XY",
        sub_place_of_perform_county_code="004",
        sub_place_of_perform_county_name="COUNTYSVILLE",
        sub_place_of_performance_zip="12345",
        sub_place_of_perform_congressio_raw="06",
        sub_place_of_perform_congressio="06",
        sub_place_of_performance_congressional_current="90",
        action_date="2020-02-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        award_id=3,
        subaward_amount=100,
        sub_place_of_perform_country_na=None,
        sub_place_of_perform_country_co_raw="US",
        sub_place_of_perform_country_co="US",
        sub_place_of_perform_state_code="XY",
        sub_place_of_perform_county_code="001",
        sub_place_of_perform_county_name="SOMEWHEREVILLE",
        sub_place_of_performance_zip="98765",
        sub_place_of_perform_congressio_raw="90",
        sub_place_of_perform_congressio="90",
        sub_place_of_performance_congressional_current="05",
        action_date="2020-03-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        award_id=4,
        subaward_amount=1000,
        sub_place_of_perform_country_na=None,
        sub_place_of_perform_country_co_raw="US",
        sub_place_of_perform_country_co="US",
        sub_place_of_perform_state_code="XY",
        sub_place_of_perform_county_code="001",
        sub_place_of_perform_county_name="SOMEWHEREVILLE",
        sub_place_of_performance_zip="98765",
        sub_place_of_perform_congressio_raw="90",
        sub_place_of_perform_congressio="90",
        sub_place_of_performance_congressional_current="05",
        action_date="2020-04-01",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        is_fpds=True,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="004",
        pop_zip5="12345",
        pop_congressional_code="06",
        pop_congressional_code_current="90",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        is_fpds=True,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="004",
        pop_zip5="12345",
        pop_congressional_code="06",
        pop_congressional_code_current="90",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        federal_action_obligation=3,
        generated_pragmatic_obligation=3,
        action_date="2020-01-03",
        fiscal_action_date="2020-04-03",
        is_fpds=True,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="001",
        pop_zip5="98765",
        pop_congressional_code="90",
        pop_congressional_code_current="05",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        federal_action_obligation=4,
        generated_pragmatic_obligation=4,
        action_date="2020-01-04",
        fiscal_action_date="2020-04-04",
        is_fpds=True,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="001",
        pop_zip5="98765",
        pop_congressional_code="90",
        pop_congressional_code_current="05",
    )

    baker.make("recipient.StateData", name="Test State", code="XY", fips="99")
    baker.make("references.RefCountryCode", country_name="UNITED STATES", country_code="US")
    baker.make("references.PopCounty", state_code="99", county_name="SOMEWHEREVILLE", county_number="001")
    baker.make("references.PopCounty", state_code="99", county_name="COUNTYSVILLE", county_number="004")
    baker.make("references.PopCongressionalDistrict", state_code="99", congressional_district="06")
    baker.make("references.PopCongressionalDistrict", state_code="99", congressional_district="05")
    baker.make("references.PopCongressionalDistrict", state_code="99", congressional_district="90")

    code_to_state["XY"] = {"name": "Test State", "fips": "99"}
    state_to_code["Test State"] = "XY"
    fips_to_code["99"] = "XY"


@pytest.fixture
def federal_accounts_test_data(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        federal_action_obligation=1,
        generated_pragmatic_obligation=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        is_fpds=True,
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
        recipient_levels=["R"],
        federal_accounts=[{"id": 10, "account_title": "Test Federal Account", "federal_account_code": "020-0001"}],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        federal_action_obligation=2,
        generated_pragmatic_obligation=2,
        action_date="2020-01-02",
        fiscal_action_date="2020-04-02",
        is_fpds=True,
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
        recipient_levels=["R"],
        federal_accounts=[{"id": 10, "account_title": "Test Federal Account", "federal_account_code": "020-0001"}],
    )

    baker.make(
        "recipient.RecipientLookup",
        duns="000000000",
        legal_business_name="Sample Recipient",
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
    )

    baker.make(
        "recipient.RecipientProfile",
        recipient_unique_id="000000000",
        recipient_level="R",
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
        recipient_name="Sample Recipient",
    )

    baker.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=1, treasury_account_id=1)
    baker.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=2, award_id=2, treasury_account_id=1)

    baker.make("accounts.TreasuryAppropriationAccount", treasury_account_identifier=1, federal_account_id=10)

    baker.make(
        "accounts.FederalAccount",
        id=10,
        agency_identifier="020",
        main_account_code="0001",
        account_title="Test Federal Account",
        federal_account_code="020-0001",
    )


def _expected_messages():
    expected_messages = [get_time_period_message()]
    expected_messages.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. "
    )
    return expected_messages


@pytest.mark.django_db
def test_category_awarding_agency_transactions(client, agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "awarding_agency", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 15,
                "name": "Awarding Toptier Agency 1",
                "code": "TA1",
                "id": 1001,
                "agency_slug": None,
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_awarding_agency_subawards(client, agency_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "awarding_agency", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 150,
                "name": "Awarding Toptier Agency 3",
                "code": "TA3",
                "id": 1003,
                "agency_slug": None,
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_awarding_subagency_transactions(
    client, agency_test_data, monkeypatch, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "awarding_subagency", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 15.0,
                "name": "Awarding Subtier Agency 1",
                "code": "SA1",
                "id": 1001,
                "subagency_slug": "awarding-subtier-agency-1",
                "agency_id": 2001,
                "agency_abbreviation": "TA1",
                "agency_name": "Awarding Toptier Agency 1",
                "agency_slug": "awarding-toptier-agency-1",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_awarding_subagency_subawards(client, agency_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "awarding_subagency", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 150.0,
                "name": "Awarding Subtier Agency 3",
                "code": "SA3",
                "id": 1003,
                "subagency_slug": "awarding-subtier-agency-3",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_funding_agency_transactions(client, agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "funding_agency", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 15,
                "name": "Funding Toptier Agency 2",
                "code": "TA2",
                "id": 1002,
                "total_outlays": None,
                "agency_slug": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_funding_agency_subawards(client, agency_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "funding_agency", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )
    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 150,
                "name": "Funding Toptier Agency 4",
                "code": "TA4",
                "id": 1004,
                "total_outlays": None,
                "agency_slug": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_funding_subagency_transactions(
    client, agency_test_data, monkeypatch, elasticsearch_transaction_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "funding_subagency", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 15,
                "name": "Funding Subtier Agency 2",
                "code": "SA2",
                "id": 1002,
                "subagency_slug": "funding-subtier-agency-2",
                "agency_id": 2002,
                "agency_abbreviation": "TA2",
                "agency_name": "Funding Toptier Agency 2",
                "agency_slug": "funding-toptier-agency-2",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_funding_subagency_subawards(client, agency_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "funding_subagency", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )
    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 150,
                "name": "Funding Subtier Agency 4",
                "code": "SA4",
                "id": 1004,
                "subagency_slug": "funding-subtier-agency-4",
                "agency_id": 2004,
                "agency_abbreviation": "TA4",
                "agency_name": "Funding Toptier Agency 4",
                "agency_slug": "funding-toptier-agency-4",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_recipient_transactions(client, recipient_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "recipient", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )
    expected_response = {
        "category": "recipient",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 15,
                "name": "MULTIPLE RECIPIENTS",
                "code": None,
                "recipient_id": None,
                "uei": None,
                "total_outlays": None,
            },
            {
                "amount": 11,
                "name": "JOHN DOE",
                "code": "1234JD4321",
                "recipient_id": "0b54895d-2393-ea12-48e3-deae990614d9-C",
                "uei": None,
                "total_outlays": None,
            },
            {
                "amount": 2,
                "name": "UNIVERSITY OF PAWNEE",
                "code": "00UOP00",
                "recipient_id": "2af2a5a5-3126-2c76-3681-dec2cf148f1a-P",
                "uei": None,
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_recipient_subawards(client, recipient_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {"category": "recipient", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "recipient",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10000,
                "code": None,
                "name": "MULTIPLE RECIPIENTS",
                "recipient_id": None,
                "total_outlays": None,
                "uei": None,
            },
            {
                "amount": 1100,
                "code": "1234JD4321",
                "recipient_id": "0b54895d-2393-ea12-48e3-deae990614d9-C",
                "name": "JOHN DOE",
                "total_outlays": None,
                "uei": None,
            },
            {
                "amount": 11,
                "code": "00UOP00",
                "recipient_id": "2af2a5a5-3126-2c76-3681-dec2cf148f1a-P",
                "name": "UNIVERSITY OF PAWNEE",
                "total_outlays": None,
                "uei": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_recipient_duns_subawards_deprecated(
    client, recipient_test_data, monkeypatch, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "recipient_duns", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )
    expected_response = {
        "category": "recipient_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10000,
                "code": None,
                "name": "MULTIPLE RECIPIENTS",
                "recipient_id": None,
                "total_outlays": None,
                "uei": None,
            },
            {
                "amount": 1100,
                "code": "1234JD4321",
                "recipient_id": "0b54895d-2393-ea12-48e3-deae990614d9-C",
                "name": "JOHN DOE",
                "total_outlays": None,
                "uei": None,
            },
            {
                "amount": 11,
                "code": "00UOP00",
                "recipient_id": "2af2a5a5-3126-2c76-3681-dec2cf148f1a-P",
                "name": "UNIVERSITY OF PAWNEE",
                "total_outlays": None,
                "uei": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_cfda_transactions(client, cfda_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "cfda", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1, "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_cfda_subawards(client, cfda_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {"category": "cfda", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1, "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_defc_subawards(client, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"category": "defc", "spending_level": "subawards", "page": 1, "limit": 10}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0


@pytest.mark.django_db
def test_category_defc_awards(client, monkeypatch, elasticsearch_transaction_index):
    # TODO: This test should be updated to include data
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"category": "defc", "spending_level": "transactions", "page": 1, "limit": 10}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0


@pytest.mark.django_db
def test_category_psc_transactions(client, psc_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "psc", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "psc",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "9876", "id": None, "name": "PSC DESCRIPTION DOWN", "total_outlays": None},
            {"amount": 2, "code": "1234", "id": None, "name": "PSC DESCRIPTION UP", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_naics_transactions(client, naics_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "naics", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "naics",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "NAICS 9876", "name": "SOURCE NAICS DESC 9876", "id": None, "total_outlays": None},
            {"amount": 2, "code": "NAICS 1234", "name": "SOURCE NAICS DESC 1234", "id": None, "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_county_transactions(client, geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "county", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 7, "code": "001", "name": "SOMEWHEREVILLE", "id": None, "total_outlays": None},
            {"amount": 3, "code": "004", "name": "COUNTYSVILLE", "id": None, "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_county_subawards(client, geo_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {"category": "county", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 1100, "code": "001", "id": None, "name": "SOMEWHEREVILLE", "total_outlays": None},
            {"amount": 11, "code": "004", "id": None, "name": "COUNTYSVILLE", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_district_transactions(client, geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "district", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 7, "code": "05", "name": "XY-05", "id": None, "total_outlays": None},
            {"amount": 3, "code": "90", "name": "XY-MULTIPLE DISTRICTS", "id": None, "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_district_subawards(client, geo_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {"category": "district", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 1100, "code": "05", "id": None, "name": "XY-05", "total_outlays": None},
            {"amount": 11, "code": "90", "id": None, "name": "XY-MULTIPLE DISTRICTS", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_state_territory(client, geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "state_territory", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 10, "code": "XY", "name": "Test State", "id": None, "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_state_territory_subawards(client, geo_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {"category": "state_territory", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 1111, "code": "XY", "id": None, "name": "Test State", "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_country(client, geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "country", "spending_level": "transactions", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 10, "code": "US", "name": "UNITED STATES", "id": None, "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_country_subawards(client, geo_test_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {"category": "country", "spending_level": "subawards", "page": 1, "limit": 50}
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 1111, "code": "US", "id": None, "name": "UNITED STATES", "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "subawards",
    }

    assert response.json() == expected_response


@pytest.mark.django_db
def test_category_federal_accounts(client, federal_accounts_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {
        "category": "federal_account",
        "filters": {"recipient_id": "ab4d44f6-7a16-4ca7-405a-dcb913effbaf-R"},
        "spending_level": "transactions",
        "page": 1,
        "limit": 50,
    }
    response = client.post(
        "/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps(test_payload)
    )

    expected_response = {
        "category": "federal_account",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 3, "code": "020-0001", "name": "Test Federal Account", "id": 10, "total_outlays": None}],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert response.json() == expected_response
