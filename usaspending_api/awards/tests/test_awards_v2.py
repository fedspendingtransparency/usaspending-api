import datetime
import pytest
import json

from rest_framework import status
from model_mommy import mommy

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.references.models import Agency, Location, ToptierAgency, SubtierAgency, OfficeAgency, LegalEntity


@pytest.fixture
def awards_and_transactions(db):
    parent_loc = {
        "pk": 2,
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "SC",
        "city_name": "Charleston",
        "county_name": "CHARLESTON",
        "address_line1": "123 calhoun st",
        "address_line2": None,
        "address_line3": None,
        "zip4": 294245897,
        "congressional_code": "90",
        "zip5": 29424,
        "foreign_postal_code": None,
        "foreign_province": None,
    }
    loc = {
        "pk": 1,
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "NC",
        "city_name": "Charlotte",
        "county_name": "BUNCOMBE",
        "address_line1": "123 main st",
        "address_line2": None,
        "address_line3": None,
        "zip4": 122045312,
        "congressional_code": "90",
        "zip5": 12204,
        "foreign_postal_code": None,
        "foreign_province": None,
    }

    sub_agency = {"pk": 1, "name": "agency name", "abbreviation": "some other stuff"}
    trans_asst = {"pk": 1, "award_id": 1}
    trans_asst2 = {"pk": 3, "award_id": 3}
    trans_asst3 = {"pk": 4, "award_id": 3}
    trans_asst4 = {"pk": 5, "award_id": 3}
    trans_cont = {"pk": 2, "award_id": 2}
    duns = {"awardee_or_recipient_uniqu": "123", "legal_business_name": "Sams Club"}
    parent_recipient_lookup = {"duns": "123", "recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc"}
    recipient_lookup = {"duns": "456", "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367"}
    parent_recipient_profile = {"recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc", "recipient_level": "P"}
    recipient_profile = {"recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367", "recipient_level": "C"}
    mommy.make("references.Cfda", program_number=12.340)
    mommy.make(
        "references.Cfda",
        program_number=10.001,
        program_title="CFDA Title",
        objectives="objectives",
        obligations="whatever",
        popular_name="Popular",
        url="www.website.com",
        website_address="www.website.biz",
        federal_agency="Agency 1",
    )
    mommy.make(
        "references.Cfda",
        program_number=10.002,
        program_title="CFDA Title 2",
        obligations="whatever",
        objectives="objectives",
        popular_name="Popular",
        url="www.website.com",
        website_address="www.website.biz",
        federal_agency="Agency 2",
    )
    mommy.make("references.Location", **parent_loc)
    mommy.make("references.Location", **loc)
    mommy.make("recipient.DUNS", **duns)
    mommy.make("recipient.RecipientLookup", **parent_recipient_lookup)
    mommy.make("recipient.RecipientLookup", **recipient_lookup)
    mommy.make("recipient.RecipientProfile", **parent_recipient_profile)
    mommy.make("recipient.RecipientProfile", **recipient_profile)
    mommy.make("references.SubtierAgency", **sub_agency)
    mommy.make("references.ToptierAgency", **sub_agency)
    mommy.make("references.OfficeAgency", name="office_agency", office_agency_id=1)

    parent_le = {
        "pk": 2,
        "recipient_name": "Dave's Pizza LLC",
        "recipient_unique_id": "123",
        "business_categories": ["limited liability"],
        "location": Location.objects.get(pk=2),
    }

    le = {"pk": 1, "business_categories": ["small_business"]}

    ag = {
        "pk": 1,
        "toptier_agency": ToptierAgency.objects.get(pk=1),
        "subtier_agency": SubtierAgency.objects.get(pk=1),
        "office_agency": OfficeAgency.objects.get(pk=1),
    }

    mommy.make("awards.TransactionNormalized", **trans_asst)
    mommy.make("awards.TransactionNormalized", **trans_asst2)
    mommy.make("awards.TransactionNormalized", **trans_asst3)
    mommy.make("awards.TransactionNormalized", **trans_asst4)
    mommy.make("awards.TransactionNormalized", **trans_cont)
    mommy.make("references.Agency", **ag)
    mommy.make("references.LegalEntity", **parent_le)
    mommy.make("references.LegalEntity", **le)

    asst_data = {
        "pk": 1,
        "transaction": TransactionNormalized.objects.get(pk=1),
        "cfda_number": 12.340,
        "cfda_title": "Shiloh",
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_state_code": "NC",
        "legal_entity_city_name": "Charlotte",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_congressional": "90",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_na": "Tripoli",
        "place_of_performance_city": "Austin",
        "place_of_perfor_state_code": "TX",
        "place_of_performance_congr": "-0-",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_zip5": "40221",
        "place_of_performance_forei": None,
        "modified_at": "2000-01-02T00:00:00Z",
        "awarding_office_name": "awarding_office",
        "funding_office_name": "funding_office",
        "officer_1_name": "John Apple",
        "officer_1_amount": 50000.00,
        "officer_2_name": "Wally World",
        "officer_2_amount": 4623.00,
    }

    asst_data2 = {
        "pk": 3,
        "transaction": TransactionNormalized.objects.get(pk=3),
        "cfda_number": 10.001,
        "federal_action_obligation": 100,
        "non_federal_funding_amount": 0,
        "total_funding_amount": 100,
        "cfda_title": "CFDA Title",
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_state_code": "NC",
        "legal_entity_city_name": "Charlotte",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_congressional": "90",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_na": "Tripoli",
        "place_of_performance_city": "Austin",
        "place_of_perfor_state_code": "TX",
        "place_of_performance_congr": "-0-",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_zip5": "40221",
        "place_of_performance_forei": None,
        "modified_at": "2000-01-02T00:00:00Z",
        "awarding_office_name": "awarding_office",
        "funding_office_name": "funding_office",
        "officer_1_name": "John Apple",
        "officer_1_amount": 50000.00,
        "officer_2_name": "Wally World",
        "officer_2_amount": 4623.00,
    }

    asst_data3 = {
        "pk": 4,
        "transaction": TransactionNormalized.objects.get(pk=4),
        "cfda_number": "10.002",
        "cfda_title": "CFDA Title 2",
        "federal_action_obligation": 400,
        "non_federal_funding_amount": 0,
        "total_funding_amount": 400,
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_state_code": "NC",
        "legal_entity_city_name": "Charlotte",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_congressional": "90",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_na": "Tripoli",
        "place_of_performance_city": "Austin",
        "place_of_perfor_state_code": "TX",
        "place_of_performance_congr": "-0-",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_zip5": "40221",
        "place_of_performance_forei": None,
        "modified_at": "2000-01-02T00:00:00Z",
        "awarding_office_name": "awarding_office",
        "funding_office_name": "funding_office",
        "officer_1_name": "John Apple",
        "officer_1_amount": 50000.00,
        "officer_2_name": "Wally World",
        "officer_2_amount": 4623.00,
    }

    asst_data4 = {
        "pk": 5,
        "transaction": TransactionNormalized.objects.get(pk=5),
        "cfda_number": "10.002",
        "cfda_title": "CFDA Title 2",
        "federal_action_obligation": 100,
        "non_federal_funding_amount": 0,
        "total_funding_amount": 100,
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_state_code": "NC",
        "legal_entity_city_name": "Charlotte",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_congressional": "90",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_na": "Tripoli",
        "place_of_performance_city": "Austin",
        "place_of_perfor_state_code": "TX",
        "place_of_performance_congr": "-0-",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_zip5": "40221",
        "place_of_performance_forei": None,
        "modified_at": "2000-01-02T00:00:00Z",
        "awarding_office_name": "awarding_office",
        "funding_office_name": "funding_office",
        "officer_1_name": "John Apple",
        "officer_1_amount": 50000.00,
        "officer_2_name": "Wally World",
        "officer_2_amount": 4623.00,
    }

    cont_data = {
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "clinger_cohen_act_pla_desc": "NO",
        "commercial_item_acquisitio": "A",
        "commercial_item_test_desc": "NO",
        "consolidated_contract_desc": "NOT CONSOLIDATED",
        "construction_wage_rat_desc": "NO",
        "cost_or_pricing_data_desc": "NO",
        "dod_claimant_program_code": "C9E",
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "extent_competed": "D",
        "fair_opportunity_limi_desc": None,
        "fed_biz_opps_description": "YES",
        "foreign_funding_desc": "NOT APPLICABLE",
        "idv_type_description": None,
        "information_technolog_desc": "NOT IT PRODUCTS OR SERVICES",
        "interagency_contract_desc": "NOT APPLICABLE",
        "labor_standards_descrip": "NO",
        "last_modified": "2001-02-03",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": "Charlotte",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_zip5": "12204",
        "legal_entity_zip_last4": "5312",
        "major_program": None,
        "materials_supplies_descrip": "NO",
        "multi_year_contract_desc": "NO",
        "multiple_or_single_aw_desc": None,
        "naics": "333911",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "other_than_full_and_o_desc": None,
        "pk": 2,
        "place_of_perf_country_desc": "UNITED STATES",
        "place_of_perform_city_name": "Charlotte",
        "place_of_perform_country_c": "USA",
        "place_of_perform_county_na": "BUNCOMBE",
        "place_of_performance_congr": "90",
        "place_of_performance_state": "NC",
        "place_of_perform_zip_last4": "5312",
        "place_of_performance_zip5": "12204",
        "period_of_perf_potential_e": "2003-04-05",
        "price_evaluation_adjustmen": None,
        "product_or_service_co_desc": None,
        "product_or_service_code": "4730",
        "program_acronym": None,
        "program_system_or_equipmen": "000",
        "purchase_card_as_paym_desc": "NO",
        "referenced_idv_agency_iden": "9700",
        "referenced_idv_agency_desc": "whatever",
        "sea_transportation_desc": "NO",
        "small_business_competitive": False,
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "subcontracting_plan": "B",
        "transaction": TransactionNormalized.objects.get(pk=2),
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "type_of_idc_description": None,
        "type_set_aside_description": None,
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
        "awarding_office_name": "awarding_office",
        "funding_office_name": "funding_office",
        "officer_1_name": "Tom",
        "officer_1_amount": 10000.00,
        "officer_2_name": "Stan Burger",
        "officer_2_amount": 1234.00,
    }
    mommy.make("awards.TransactionFABS", **asst_data)
    mommy.make("awards.TransactionFABS", **asst_data2)
    mommy.make("awards.TransactionFABS", **asst_data3)
    mommy.make("awards.TransactionFABS", **asst_data4)
    mommy.make("awards.TransactionFPDS", **cont_data)
    award_1_model = {
        "pk": 1,
        "type": "11",
        "type_description": "OTHER FINANCIAL ASSISTANCE",
        "category": "grant",
        "uri": 1234,
        "description": "lorem ipsum",
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
        "generated_unique_award_id": "ASST_AGG_1830212.0481163_3620",
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "recipient": LegalEntity.objects.get(pk=1),
        "place_of_performance": Location.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=1),
        "date_signed": "2005-04-03",
    }

    award_2_model = {
        "pk": 2,
        "type": "A",
        "type_description": "DEFINITIVE CONTRACT",
        "category": "contract",
        "piid": "5678",
        "parent_award_piid": "1234",
        "description": "lorem ipsum",
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "recipient": LegalEntity.objects.get(pk=1),
        "total_obligation": 1000,
        "base_and_all_options_value": 2000,
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
        "generated_unique_award_id": "CONT_AWD_03VD_9700_SPM30012D3486_9700",
        "place_of_performance": Location.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=2),
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
        "date_signed": "2004-03-02",
    }

    award_3_model = {
        "pk": 3,
        "type": "03",
        "type_description": "FORMULA GRANT (A)",
        "category": "grant",
        "piid": "0001",
        "parent_award_piid": "0001",
        "description": "lorem ipsum",
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "recipient": LegalEntity.objects.get(pk=1),
        "total_obligation": 600,
        "base_and_all_options_value": 600,
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
        "generated_unique_award_id": "",
        "place_of_performance": Location.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=3),
        "total_subaward_amount": 0,
        "subaward_count": 0,
        "date_signed": "2004-03-02",
    }

    mommy.make("awards.Award", **award_1_model)
    mommy.make("awards.Award", **award_2_model)
    mommy.make("awards.Award", **award_3_model)


@pytest.fixture
def update_awards(db):
    mommy.make("awards.Award", pk=11)
    mommy.make("awards.Award", pk=12)


def test_award_last_updated_endpoint(client, update_awards):
    """Test the awards endpoint."""
    resp = client.get("/api/v2/awards/last_updated/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["last_updated"] == datetime.datetime.now().strftime("%m/%d/%Y")


def test_award_endpoint_generated_id(client, awards_and_transactions):

    resp = client.get("/api/v2/awards/ASST_AGG_1830212.0481163_3620/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_asst

    resp = client.get("/api/v2/awards/CONT_AWD_03VD_9700_SPM30012D3486_9700/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_cont

    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_asst

    resp = client.get("/api/v2/awards/2/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_cont


def test_award_mulitple_cfdas(client, awards_and_transactions):

    resp = client.get("/api/v2/awards/3/")
    print(json.loads(resp.content.decode("utf-8"))["cfda_info"])
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["cfda_info"] == [
        {
            "cfda_objectives": "objectives",
            "cfda_number": "10.002",
            "cfda_title": "CFDA Title 2",
            "federal_action_obligation_amount": 500.0,
            "non_federal_funding_amount": 0.0,
            "total_funding_amount": 500.0,
            "cfda_federal_agency": "Agency 2",
            "cfda_website": "www.website.biz",
            "sam_website": "www.website.com",
            "cfda_obligations": "whatever",
            "cfda_popular_name": "Popular",
        },
        {
            "cfda_objectives": "objectives",
            "cfda_number": "10.001",
            "cfda_title": "CFDA Title",
            "federal_action_obligation_amount": 100.0,
            "non_federal_funding_amount": 0.0,
            "total_funding_amount": 100.0,
            "cfda_federal_agency": "Agency 1",
            "cfda_website": "www.website.biz",
            "sam_website": "www.website.com",
            "cfda_obligations": "whatever",
            "cfda_popular_name": "Popular",
        },
    ]


expected_response_asst = {
    "id": 1,
    "type": "11",
    "category": "grant",
    "type_description": "OTHER FINANCIAL ASSISTANCE",
    "uri": "1234",
    "fain": None,
    "generated_unique_award_id": "ASST_AGG_1830212.0481163_3620",
    "description": "lorem ipsum",
    "cfda_info": [
        {
            "cfda_objectives": None,
            "cfda_number": "12.340",
            "cfda_title": None,
            "federal_action_obligation_amount": 0.0,
            "non_federal_funding_amount": 0.0,
            "total_funding_amount": 0.0,
            "cfda_federal_agency": None,
            "cfda_website": None,
            "sam_website": None,
            "cfda_obligations": None,
            "cfda_popular_name": None,
        }
    ],
    "base_and_all_options": None,
    "base_exercised_options": None,
    "non_federal_funding": None,
    "total_funding": None,
    "total_loan_value": None,
    "total_obligation": None,
    "total_subsidy_cost": None,
    "transaction_obligated_amount": None,
    "awarding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "awarding_office",
    },
    "funding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "funding_office",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367-C",
        "recipient_name": "John's Pizza",
        "recipient_unique_id": "456",
        "parent_recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P",
        "parent_recipient_name": "Dave's Pizza LLC",
        "parent_recipient_unique_id": "123",
        "business_categories": ["Small Business"],
        "location": {
            "address_line1": "123 main st",
            "address_line2": None,
            "address_line3": None,
            "foreign_province": None,
            "city_name": "Charlotte",
            "county_name": "BUNCOMBE",
            "state_code": "NC",
            "zip5": "12204",
            "zip4": "5312",
            "foreign_postal_code": None,
            "country_name": "UNITED STATES",
            "location_country_code": "USA",
            "congressional_code": "90",
        },
    },
    "subaward_count": 10,
    "total_subaward_amount": 12345.0,
    "executive_details": {
        "officers": [{"name": "John Apple", "amount": 50000.00}, {"name": "Wally World", "amount": 4623.00}]
    },
    "period_of_performance": {"start_date": "2004-02-04", "end_date": "2005-02-04", "last_modified_date": "2000-01-02"},
    "place_of_performance": {
        "address_line1": None,
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Austin",
        "county_name": "Tripoli",
        "state_code": "TX",
        "state_name": "Texas",
        "zip5": "40221",
        "zip4": "2135",
        "foreign_postal_code": None,
        "country_name": "Pacific Delta Amazon",
        "location_country_code": "PDA",
        "congressional_code": "-0-",
    },
    "date_signed": "2005-04-03",
}


expected_response_cont = {
    "id": 2,
    "generated_unique_award_id": "CONT_AWD_03VD_9700_SPM30012D3486_9700",
    "type": "A",
    "category": "contract",
    "type_description": "DEFINITIVE CONTRACT",
    "piid": "5678",
    "parent_award_piid": "1234",
    "description": "lorem ipsum",
    "awarding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "awarding_office",
    },
    "funding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "funding_office",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367-C",
        "recipient_name": "John's Pizza",
        "recipient_unique_id": "456",
        "parent_recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P",
        "parent_recipient_name": "Dave's Pizza LLC",
        "parent_recipient_unique_id": "123",
        "business_categories": ["Small Business"],
        "location": {
            "address_line1": "123 main st",
            "address_line2": None,
            "address_line3": None,
            "foreign_province": None,
            "city_name": "Charlotte",
            "county_name": "BUNCOMBE",
            "state_code": "NC",
            "zip5": "12204",
            "zip4": "5312",
            "foreign_postal_code": None,
            "country_name": "UNITED STATES",
            "location_country_code": "USA",
            "congressional_code": "90",
        },
    },
    "total_obligation": 1000.0,
    "base_and_all_options": 2000.0,
    "base_exercised_options": None,
    "period_of_performance": {
        "start_date": "2004-02-04",
        "end_date": "2005-02-04",
        "last_modified_date": "2001-02-03",
        "potential_end_date": "2003-04-05",
    },
    "place_of_performance": {
        "address_line1": None,
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Charlotte",
        "county_name": "BUNCOMBE",
        "state_code": "NC",
        "state_name": "North Carolina",
        "zip5": "12204",
        "zip4": "5312",
        "foreign_postal_code": None,
        "country_name": "UNITED STATES",
        "location_country_code": "USA",
        "congressional_code": "90",
    },
    "latest_transaction_contract_data": {
        "clinger_cohen_act_planning": None,
        "clinger_cohen_act_planning_description": "NO",
        "commercial_item_acquisition": "A",
        "commercial_item_acquisition_description": None,
        "commercial_item_test_program": None,
        "commercial_item_test_program_description": "NO",
        "consolidated_contract": None,
        "consolidated_contract_description": "NOT CONSOLIDATED",
        "construction_wage_rate": None,
        "construction_wage_rate_description": "NO",
        "cost_or_pricing_data": None,
        "cost_or_pricing_data_description": "NO",
        "dod_claimant_program": "C9E",
        "dod_claimant_program_description": None,
        "domestic_or_foreign_entity": None,
        "domestic_or_foreign_entity_description": "U.S. OWNED BUSINESS",
        "evaluated_preference": None,
        "evaluated_preference_description": "NO PREFERENCE USED",
        "extent_competed": "D",
        "extent_competed_description": None,
        "fair_opportunity_limited": None,
        "fair_opportunity_limited_description": None,
        "fed_biz_opps": None,
        "fed_biz_opps_description": "YES",
        "foreign_funding": None,
        "foreign_funding_description": "NOT APPLICABLE",
        "idv_type_description": None,
        "interagency_contracting_authority": None,
        "interagency_contracting_authority_description": "NOT APPLICABLE",
        "labor_standards": None,
        "labor_standards_description": "NO",
        "major_program": None,
        "materials_supplies": None,
        "materials_supplies_description": "NO",
        "multi_year_contract": None,
        "multi_year_contract_description": "NO",
        "multiple_or_single_award_description": None,
        "naics": "333911",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "other_than_full_and_open": None,
        "other_than_full_and_open_description": None,
        "price_evaluation_adjustment": None,
        "dod_acquisition_program": "000",
        "dod_acquisition_program_description": None,
        "information_technology_commercial_item_category": None,
        "information_technology_commercial_item_category_description": "NOT IT PRODUCTS OR SERVICES",
        "product_or_service_code": "4730",
        "product_or_service_description": None,
        "program_acronym": None,
        "purchase_card_as_payment_method": None,
        "purchase_card_as_payment_method_description": "NO",
        "referenced_idv_agency_iden": "9700",
        "referenced_idv_agency_desc": "whatever",
        "sea_transportation": None,
        "sea_transportation_description": "NO",
        "small_business_competitive": False,
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "solicitation_procedures_description": None,
        "subcontracting_plan": "B",
        "subcontracting_plan_description": None,
        "type_of_contract_pricing": None,
        "type_of_contract_pricing_description": "FIRM FIXED PRICE",
        "type_of_idc_description": None,
        "type_set_aside": None,
        "type_set_aside_description": None,
    },
    "subaward_count": 10,
    "total_subaward_amount": 12345.0,
    "executive_details": {
        "officers": [{"name": "Tom", "amount": 10000.00}, {"name": "Stan Burger", "amount": 1234.00}]
    },
    "date_signed": "2004-03-02",
}
