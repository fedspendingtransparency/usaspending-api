import pytest

from model_mommy import mommy

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_agency_types import (
    AwardingAgencyViewSet,
    AwardingSubagencyViewSet,
    FundingAgencyViewSet,
    FundingSubagencyViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_federal_account import FederalAccountViewSet
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_industry_codes import (
    CfdaViewSet,
    PSCViewSet,
    NAICSViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_locations import (
    CountyViewSet,
    DistrictViewSet,
    StateTerritoryViewSet,
    CountryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient_duns import RecipientDunsViewSet


@pytest.fixture
def psc_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)
    mommy.make("awards.Award", id=3, latest_transaction_id=3)
    mommy.make("awards.Award", id=4, latest_transaction_id=4)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        is_fpds=True,
        federal_action_obligation=1,
        action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        is_fpds=True,
        federal_action_obligation=1,
        action_date="2020-01-02",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        is_fpds=True,
        federal_action_obligation=2,
        action_date="2020-01-03",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        is_fpds=True,
        federal_action_obligation=2,
        action_date="2020-01-04",
    )

    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        product_or_service_code="1234",
        product_or_service_co_desc="PSC DESCRIPTION UP",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        product_or_service_code="1234",
        product_or_service_co_desc="PSC DESCRIPTION UP",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        product_or_service_code="9876",
        product_or_service_co_desc="PSC DESCRIPTION DOWN",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        product_or_service_code="9876",
        product_or_service_co_desc="PSC DESCRIPTION DOWN",
    )

    mommy.make("references.PSC", code="1234", description="PSC DESCRIPTION UP")
    mommy.make("references.PSC", code="9876", description="PSC DESCRIPTION DOWN")


@pytest.fixture
def cfda_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.Subaward", id=1, award_id=1, amount=1, cfda_id=1, cfda_number="CFDA1234", cfda_title="CFDA TITLE 1234"
    )
    mommy.make(
        "awards.Subaward", id=2, award_id=2, amount=1, cfda_id=1, cfda_number="CFDA1234", cfda_title="CFDA TITLE 1234"
    )

    mommy.make("awards.TransactionNormalized", id=1, award_id=1, federal_action_obligation=1, action_date="2020-01-01")
    mommy.make("awards.TransactionNormalized", id=2, award_id=2, federal_action_obligation=1, action_date="2020-01-02")

    mommy.make("awards.TransactionFABS", transaction_id=1, cfda_number="CFDA1234", cfda_title="CFDA TITLE 1234")
    mommy.make("awards.TransactionFABS", transaction_id=2, cfda_number="CFDA1234", cfda_title="CFDA TITLE 1234")

    mommy.make("references.Cfda", id=1, program_number="CFDA1234", program_title="CFDA TITLE 1234")


@pytest.fixture
def naics_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)
    mommy.make("awards.Award", id=3, latest_transaction_id=3)
    mommy.make("awards.Award", id=4, latest_transaction_id=4)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        is_fpds=True,
        federal_action_obligation=1,
        action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        is_fpds=True,
        federal_action_obligation=1,
        action_date="2020-01-02",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        is_fpds=True,
        federal_action_obligation=2,
        action_date="2020-01-03",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        is_fpds=True,
        federal_action_obligation=2,
        action_date="2020-01-04",
    )

    mommy.make("awards.TransactionFPDS", transaction_id=1, naics="NAICS 1234", naics_description="NAICS DESC 1234")
    mommy.make("awards.TransactionFPDS", transaction_id=2, naics="NAICS 1234", naics_description="NAICS DESC 1234")
    mommy.make("awards.TransactionFPDS", transaction_id=3, naics="NAICS 9876", naics_description="NAICS DESC 9876")
    mommy.make("awards.TransactionFPDS", transaction_id=4, naics="NAICS 9876", naics_description="NAICS DESC 9876")

    mommy.make("references.NAICS", code="NAICS 1234", description="SOURCE NAICS DESC 1234", year=1955)
    mommy.make("references.NAICS", code="NAICS 9876", description="SOURCE NAICS DESC 9876", year=1985)


@pytest.fixture
def agency_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.Subaward",
        id=1,
        latest_transaction_id=1,
        amount=50,
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
    )
    mommy.make(
        "awards.Subaward",
        id=2,
        latest_transaction_id=2,
        amount=100,
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
    )

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1002,
        federal_action_obligation=5,
        action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        awarding_agency_id=1001,
        funding_agency_id=1002,
        federal_action_obligation=10,
        action_date="2020-01-02",
    )

    mommy.make("references.ToptierAgency", toptier_agency_id=2001, name="Awarding Toptier Agency 1", abbreviation="TA1")
    mommy.make("references.SubtierAgency", subtier_agency_id=3001, name="Awarding Subtier Agency 1", abbreviation="SA1")
    mommy.make("references.ToptierAgency", toptier_agency_id=2003, name="Awarding Toptier Agency 3", abbreviation="TA3")
    mommy.make("references.SubtierAgency", subtier_agency_id=3003, name="Awarding Subtier Agency 3", abbreviation="SA3")

    mommy.make("references.ToptierAgency", toptier_agency_id=2002, name="Funding Toptier Agency 2", abbreviation="TA2")
    mommy.make("references.SubtierAgency", subtier_agency_id=3002, name="Funding Subtier Agency 2", abbreviation="SA2")
    mommy.make("references.ToptierAgency", toptier_agency_id=2004, name="Funding Toptier Agency 4", abbreviation="TA4")
    mommy.make("references.SubtierAgency", subtier_agency_id=3004, name="Funding Subtier Agency 4", abbreviation="SA4")

    mommy.make("references.Agency", id=1001, toptier_agency_id=2001, subtier_agency_id=3001, toptier_flag=True)
    mommy.make("references.Agency", id=1002, toptier_agency_id=2002, subtier_agency_id=3002, toptier_flag=True)

    mommy.make("references.Agency", id=1003, toptier_agency_id=2003, subtier_agency_id=3003, toptier_flag=True)
    mommy.make("references.Agency", id=1004, toptier_agency_id=2004, subtier_agency_id=3004, toptier_flag=True)


@pytest.fixture
def recipient_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)
    mommy.make("awards.Award", id=3, latest_transaction_id=3)
    mommy.make("awards.Award", id=4, latest_transaction_id=4)
    mommy.make("awards.Award", id=5, latest_transaction_id=5)

    mommy.make(
        "awards.Subaward",
        id=1,
        award_id=1,
        amount=1,
        recipient_name="University of Pawnee",
        recipient_unique_id="00UOP00",
    )
    mommy.make(
        "awards.Subaward",
        id=2,
        award_id=2,
        amount=10,
        recipient_name="University of Pawnee",
        recipient_unique_id="00UOP00",
    )
    mommy.make(
        "awards.Subaward", id=3, award_id=3, amount=100, recipient_name="John Doe", recipient_unique_id="1234JD4321"
    )
    mommy.make(
        "awards.Subaward", id=4, award_id=4, amount=1000, recipient_name="John Doe", recipient_unique_id="1234JD4321"
    )
    mommy.make(
        "awards.Subaward",
        id=5,
        award_id=5,
        amount=10000,
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id=None,
    )

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        federal_action_obligation=1,
        action_date="2020-01-01",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        federal_action_obligation=1,
        action_date="2020-01-02",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        federal_action_obligation=1,
        action_date="2020-01-03",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        federal_action_obligation=10,
        action_date="2020-01-04",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=5,
        award_id=5,
        federal_action_obligation=15,
        action_date="2020-01-05",
        is_fpds=True,
    )

    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        awardee_or_recipient_legal="University of Pawnee",
        awardee_or_recipient_uniqu="00UOP00",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        awardee_or_recipient_legal="University of Pawnee",
        awardee_or_recipient_uniqu="00UOP00",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        awardee_or_recipient_legal="John Doe",
        awardee_or_recipient_uniqu="1234JD4321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        awardee_or_recipient_legal="John Doe",
        awardee_or_recipient_uniqu="1234JD4321",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=5,
        awardee_or_recipient_legal="MULTIPLE RECIPIENTS",
        awardee_or_recipient_uniqu=None,
    )

    mommy.make(
        "recipient.RecipientLookup",
        duns="00UOP00",
        legal_business_name="University of Pawnee",
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
    )
    mommy.make(
        "recipient.RecipientLookup",
        duns="1234JD4321",
        legal_business_name="John Doe",
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
    )
    mommy.make(
        "recipient.RecipientLookup",
        duns=None,
        legal_business_name="MULTIPLE RECIPIENTS",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
    )

    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id="00UOP00",
        recipient_level="P",
        recipient_hash="2af2a5a5-3126-2c76-3681-dec2cf148f1a",
        recipient_name="University of Pawnee",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id="1234JD4321",
        recipient_level="C",
        recipient_hash="0b54895d-2393-ea12-48e3-deae990614d9",
        recipient_name="John Doe",
    )
    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id=None,
        recipient_level="R",
        recipient_hash="64af1cb7-993c-b64b-1c58-f5289af014c0",
        recipient_name="MULTIPLE RECIPIENTS",
    )


@pytest.fixture
def geo_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)
    mommy.make("awards.Award", id=3, latest_transaction_id=3)
    mommy.make("awards.Award", id=4, latest_transaction_id=4)

    mommy.make(
        "awards.Subaward",
        id=1,
        award_id=1,
        amount=1,
        pop_country_name=None,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="04",
        pop_county_name="COUNTYSVILLE",
        pop_zip4="12345",
        pop_congressional_code="06",
    )
    mommy.make(
        "awards.Subaward",
        id=2,
        award_id=2,
        amount=10,
        pop_country_name=None,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="04",
        pop_county_name="COUNTYSVILLE",
        pop_zip4="12345",
        pop_congressional_code="06",
    )
    mommy.make(
        "awards.Subaward",
        id=3,
        award_id=3,
        amount=100,
        pop_country_name=None,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="01",
        pop_county_name="SOMEWHEREVILLE",
        pop_zip4="98765",
        pop_congressional_code="90",
    )
    mommy.make(
        "awards.Subaward",
        id=4,
        award_id=4,
        amount=1000,
        pop_country_name=None,
        pop_country_code="US",
        pop_state_code="XY",
        pop_county_code="01",
        pop_county_name="SOMEWHEREVILLE",
        pop_zip4="98765",
        pop_congressional_code="90",
    )

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        federal_action_obligation=1,
        action_date="2020-01-01",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        federal_action_obligation=2,
        action_date="2020-01-02",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        federal_action_obligation=3,
        action_date="2020-01-03",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        federal_action_obligation=4,
        action_date="2020-01-04",
        is_fpds=True,
    )

    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        place_of_perf_country_desc=None,
        place_of_perform_country_c="US",
        place_of_performance_state="XY",
        place_of_perform_county_co="04",
        place_of_perform_county_na="COUNTYSVILLE",
        place_of_performance_zip5="12345",
        place_of_performance_congr="06",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        place_of_perf_country_desc=None,
        place_of_perform_country_c="US",
        place_of_performance_state="XY",
        place_of_perform_county_co="04",
        place_of_perform_county_na="COUNTYSVILLE",
        place_of_performance_zip5="12345",
        place_of_performance_congr="06",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        place_of_perf_country_desc=None,
        place_of_perform_country_c="US",
        place_of_performance_state="XY",
        place_of_perform_county_co="01",
        place_of_perform_county_na="SOMEWHEREVILLE",
        place_of_performance_zip5="98765",
        place_of_performance_congr="90",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        place_of_perf_country_desc=None,
        place_of_perform_country_c="US",
        place_of_performance_state="XY",
        place_of_perform_county_co="01",
        place_of_perform_county_na="SOMEWHEREVILLE",
        place_of_performance_zip5="98765",
        place_of_performance_congr="90",
    )

    mommy.make("recipient.StateData", name="Test State", code="XY")
    mommy.make("references.RefCountryCode", country_name="UNITED STATES", country_code="US")


@pytest.fixture
def federal_accounts_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        federal_action_obligation=1,
        action_date="2020-01-01",
        is_fpds=True,
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        federal_action_obligation=2,
        action_date="2020-01-02",
        is_fpds=True,
    )

    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        awardee_or_recipient_legal="Sample Recipient",
        awardee_or_recipient_uniqu="000000000",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        awardee_or_recipient_legal="Sample Recipient",
        awardee_or_recipient_uniqu="000000000",
    )

    mommy.make(
        "recipient.RecipientLookup",
        duns="000000000",
        legal_business_name="Sample Recipient",
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
    )

    mommy.make(
        "recipient.RecipientProfile",
        recipient_unique_id="000000000",
        recipient_level="R",
        recipient_hash="ab4d44f6-7a16-4ca7-405a-dcb913effbaf",
        recipient_name="Sample Recipient",
    )

    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=1, treasury_account_id=1)
    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=2, award_id=2, treasury_account_id=1)

    mommy.make("accounts.TreasuryAppropriationAccount", treasury_account_identifier=1, federal_account_id=10)

    mommy.make(
        "accounts.FederalAccount",
        id=10,
        agency_identifier="020",
        main_account_code="0001",
        account_title="Test Federal Account",
        federal_account_code="020-0001",
    )


def test_category_awarding_agency_awards(agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "awarding_agency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = AwardingAgencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 15, "name": "Awarding Toptier Agency 1", "code": "TA1", "id": 1001, "agency_slug": None}
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_agency_subawards(agency_test_data):
    test_payload = {"category": "awarding_agency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = AwardingAgencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "awarding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 150, "name": "Awarding Toptier Agency 3", "code": "TA3", "id": 1003}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_awards(agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "awarding_subagency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = AwardingSubagencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 15, "name": "Awarding Subtier Agency 1", "code": "SA1", "id": 1001}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_subawards(agency_test_data):
    test_payload = {"category": "awarding_subagency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = AwardingSubagencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "awarding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 150, "name": "Awarding Subtier Agency 3", "code": "SA3", "id": 1003}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_awards(agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "funding_agency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = FundingAgencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 15, "name": "Funding Toptier Agency 2", "code": "TA2", "id": 1002}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_subawards(agency_test_data):
    test_payload = {"category": "funding_agency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = FundingAgencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "funding_agency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 150, "name": "Funding Toptier Agency 4", "code": "TA4", "id": 1004}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_awards(agency_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "funding_subagency", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = FundingSubagencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 15, "name": "Funding Subtier Agency 2", "code": "SA2", "id": 1002}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_subawards(agency_test_data):
    test_payload = {"category": "funding_subagency", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = FundingSubagencyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "funding_subagency",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 150, "name": "Funding Subtier Agency 4", "code": "SA4", "id": 1004}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_recipient_duns_awards(recipient_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "recipient_duns", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = RecipientDunsViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "recipient_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 15, "name": "MULTIPLE RECIPIENTS", "code": "DUNS Number not provided", "recipient_id": None},
            {
                "amount": 11,
                "name": "JOHN DOE",
                "code": "1234JD4321",
                "recipient_id": "0b54895d-2393-ea12-48e3-deae990614d9-C",
            },
            {
                "amount": 2,
                "name": "UNIVERSITY OF PAWNEE",
                "code": "00UOP00",
                "recipient_id": "2af2a5a5-3126-2c76-3681-dec2cf148f1a-P",
            },
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_recipient_duns_subawards(recipient_test_data):
    test_payload = {"category": "recipient_duns", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = RecipientDunsViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "recipient_duns",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 10000, "code": None, "name": "MULTIPLE RECIPIENTS", "recipient_id": None},
            {
                "amount": 1100,
                "code": "1234JD4321",
                "recipient_id": "0b54895d-2393-ea12-48e3-deae990614d9-C",
                "name": "JOHN DOE",
            },
            {
                "amount": 11,
                "code": "00UOP00",
                "recipient_id": "2af2a5a5-3126-2c76-3681-dec2cf148f1a-P",
                "name": "UNIVERSITY OF PAWNEE",
            },
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_awards(cfda_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "cfda", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = CfdaViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_subawards(cfda_test_data):
    test_payload = {"category": "cfda", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = CfdaViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "cfda",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 2, "code": "CFDA1234", "name": "CFDA TITLE 1234", "id": 1}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_psc_awards(psc_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "psc", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = PSCViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "psc",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "9876", "id": None, "name": "PSC DESCRIPTION DOWN"},
            {"amount": 2, "code": "1234", "id": None, "name": "PSC DESCRIPTION UP"},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_naics_awards(naics_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "naics", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = NAICSViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "naics",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 4, "code": "NAICS 9876", "name": "SOURCE NAICS DESC 9876", "id": None},
            {"amount": 2, "code": "NAICS 1234", "name": "SOURCE NAICS DESC 1234", "id": None},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_county_awards(geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "county", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = CountyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 7, "code": "001", "name": "SOMEWHEREVILLE", "id": None},
            {"amount": 3, "code": "004", "name": "COUNTYSVILLE", "id": None},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_county_subawards(geo_test_data):
    test_payload = {"category": "county", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = CountyViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "county",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 1100, "code": "001", "id": None, "name": "SOMEWHEREVILLE"},
            {"amount": 11, "code": "004", "id": None, "name": "COUNTYSVILLE"},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_district_awards(geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "district", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = DistrictViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 7, "code": "90", "name": "XY-MULTIPLE DISTRICTS", "id": None},
            {"amount": 3, "code": "06", "name": "XY-06", "id": None},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


def test_category_district_subawards(geo_test_data):
    test_payload = {"category": "district", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = DistrictViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "district",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 1100, "code": "90", "id": None, "name": "XY-MULTIPLE DISTRICTS"},
            {"amount": 11, "code": "06", "id": None, "name": "XY-06"},
        ],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory(geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "state_territory", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = StateTerritoryViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 10, "code": "XY", "name": "Test State", "id": None}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory_subawards(geo_test_data):
    test_payload = {"category": "state_territory", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = StateTerritoryViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "state_territory",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 1111, "code": "XY", "id": None, "name": "Test State"}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country(geo_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {"category": "country", "subawards": False, "page": 1, "limit": 50}

    spending_by_category_logic = CountryViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 10, "code": "US", "name": "UNITED STATES", "id": None}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country_subawards(geo_test_data):
    test_payload = {"category": "country", "subawards": True, "page": 1, "limit": 50}

    spending_by_category_logic = CountryViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "country",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 1111, "code": "US", "id": None, "name": "UNITED STATES"}],
        "messages": [get_time_period_message()],
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_federal_accounts(federal_accounts_test_data, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_payload = {
        "category": "federal_account",
        "filters": {"recipient_id": "ab4d44f6-7a16-4ca7-405a-dcb913effbaf-R"},
        "subawards": False,
        "page": 1,
        "limit": 50,
    }

    spending_by_category_logic = FederalAccountViewSet().perform_search(test_payload, {})

    expected_response = {
        "category": "federal_account",
        "limit": 50,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 3, "code": "020-0001", "name": "Test Federal Account", "id": 10}],
        "messages": [get_time_period_message()],
    }
    assert expected_response == spending_by_category_logic
