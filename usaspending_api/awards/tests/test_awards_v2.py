import datetime
import pytest
import json

from rest_framework import status
from model_mommy import mommy

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def awards_and_transactions(db):

    # DUNS
    duns = {"awardee_or_recipient_uniqu": "123", "legal_business_name": "Sams Club"}

    mommy.make("recipient.DUNS", **duns)

    # Recipient Lookup
    parent_recipient_lookup = {"duns": "123", "recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc"}
    recipient_lookup = {"duns": "456", "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367"}

    mommy.make("recipient.RecipientLookup", **parent_recipient_lookup)
    mommy.make("recipient.RecipientLookup", **recipient_lookup)

    # Recipient Profile
    parent_recipient_profile = {"recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc", "recipient_level": "P"}
    recipient_profile = {"recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367", "recipient_level": "C"}

    mommy.make("recipient.RecipientProfile", **parent_recipient_profile)
    mommy.make("recipient.RecipientProfile", **recipient_profile)

    # CFDA
    mommy.make("references.Cfda", program_number=12.340)
    mommy.make(
        "references.Cfda",
        applicant_eligibility="Hello. I am an applicant eligibility statement.",
        beneficiary_eligibility="Hello. I am a beneficiary eligibility statement.",
        federal_agency="Agency 1",
        objectives="objectives",
        obligations="whatever",
        popular_name="Popular",
        program_number=10.001,
        program_title="CFDA Title",
        url="www.website.com",
        website_address="www.website.biz",
    )
    mommy.make(
        "references.Cfda",
        federal_agency="Agency 2",
        objectives="objectives",
        obligations="whatever",
        popular_name="Popular",
        program_number=10.002,
        program_title="CFDA Title 2",
        url="www.website.com",
        website_address="www.website.biz",
    )

    # PSC
    mommy.make("references.PSC", code="10", description="Whatever")
    mommy.make("references.PSC", code="1005", description="More specific whatever")
    mommy.make("references.PSC", code="A", description="R&D")
    mommy.make("references.PSC", code="A1", description="R&D - Steak Sauce")
    mommy.make("references.PSC", code="A13", description="R&D - Brand specific steak condiments")
    mommy.make("references.PSC", code="A136", description="R&D - Very specific steak research")
    mommy.make("references.PSC", code="M", description="Something")
    mommy.make("references.PSC", code="M1", description="Something More Specific")
    mommy.make("references.PSC", code="M123", description="Something Most Specific")

    # NAICS
    mommy.make("references.NAICS", code="11", description="Agriculture")
    mommy.make("references.NAICS", code="1111", description="Soybean & Oilseed Agriculture")
    mommy.make("references.NAICS", code="111120", description="Soybean Harvesting")

    # Toptier Agency
    toptier_agency_1 = {"pk": 1, "abbreviation": "TA1", "name": "TOPTIER AGENCY 1", "toptier_code": "ABC"}
    toptier_agency_2 = {"pk": 2, "abbreviation": "TA2", "name": "TOPTIER AGENCY 2", "toptier_code": "002"}

    mommy.make("references.ToptierAgency", **toptier_agency_1)
    mommy.make("references.ToptierAgency", **toptier_agency_2)

    # Subtier Agency
    subtier_agency_1 = {"pk": 1, "abbreviation": "SA1", "name": "SUBTIER AGENCY 1", "subtier_code": "DEF"}
    subtier_agency_2 = {"pk": 2, "abbreviation": "SA2", "name": "SUBTIER AGENCY 2", "subtier_code": "1000"}

    mommy.make("references.SubtierAgency", **subtier_agency_1)
    mommy.make("references.SubtierAgency", **subtier_agency_2)

    # Agency
    agency = {
        "pk": 1,
        "subtier_agency": SubtierAgency.objects.get(pk=1),
        "toptier_agency": ToptierAgency.objects.get(pk=1),
        "toptier_flag": True,
    }
    agency_2 = {
        "pk": 2,
        "subtier_agency": SubtierAgency.objects.get(pk=2),
        "toptier_agency": ToptierAgency.objects.get(pk=2),
        "toptier_flag": True,
    }

    mommy.make("references.Agency", **agency)
    mommy.make("references.Agency", **agency_2)

    # Transaction Normalized
    bc = {"business_categories": ["small_business"]}

    asst_trans_norm_1 = {"pk": 1, "award_id": 1, **bc}
    asst_trans_norm_2 = {"pk": 3, "award_id": 3, **bc}
    asst_trans_norm_3 = {"pk": 4, "award_id": 3, **bc}
    asst_trans_norm_4 = {"pk": 5, "award_id": 3, **bc}
    asst_trans_norm_5 = {"pk": 13, "award_id": 13, **bc}

    cont_trans_norm_1 = {"pk": 2, "award_id": 2, **bc}
    cont_trans_norm_2 = {"pk": 6, "award_id": 5, **bc}
    cont_trans_norm_3 = {"pk": 7, "award_id": 6, **bc}
    cont_trans_norm_4 = {"pk": 8, "award_id": 7, **bc}
    cont_trans_norm_5 = {"pk": 9, "award_id": 8, **bc}
    cont_trans_norm_6 = {"pk": 10, "award_id": 9, **bc}
    cont_trans_norm_7 = {"pk": 11, "award_id": 10, **bc}

    mommy.make("awards.TransactionNormalized", **asst_trans_norm_1)
    mommy.make("awards.TransactionNormalized", **asst_trans_norm_2)
    mommy.make("awards.TransactionNormalized", **asst_trans_norm_3)
    mommy.make("awards.TransactionNormalized", **asst_trans_norm_4)
    mommy.make("awards.TransactionNormalized", **asst_trans_norm_5)

    mommy.make("awards.TransactionNormalized", **cont_trans_norm_1)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_2)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_3)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_4)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_5)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_6)
    mommy.make("awards.TransactionNormalized", **cont_trans_norm_7)

    # Transaction FABS
    asst_trans_1 = {
        "pk": 1,
        "record_type": 111,
        "transaction": TransactionNormalized.objects.get(pk=1),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
        "cfda_number": 12.340,
        "cfda_title": "Shiloh",
        "funding_office_name": "funding_office",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": "Charlotte",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_name": "North Carolina",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "modified_at": "2000-01-02T00:00:00Z",
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "place_of_perfor_state_code": "TX",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_na": "Tripoli",
        "place_of_perform_county_co": "023",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_city": "Austin",
        "place_of_performance_congr": "-0-",
        "place_of_performance_forei": None,
        "place_of_performance_zip5": "40221",
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
    }
    asst_trans_2 = {
        "pk": 3,
        "record_type": 333,
        "transaction": TransactionNormalized.objects.get(pk=3),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
        "cfda_number": 10.001,
        "cfda_title": "CFDA Title",
        "federal_action_obligation": 100,
        "funding_office_name": "funding_office",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": "Charlotte",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_name": "North Carolina",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "modified_at": "2000-01-02T00:00:00Z",
        "non_federal_funding_amount": 0,
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "place_of_perfor_state_code": "TX",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_co": "023",
        "place_of_perform_county_na": "Tripoli",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_city": "Austin",
        "place_of_performance_congr": "-0-",
        "place_of_performance_forei": None,
        "place_of_performance_zip5": "40221",
        "total_funding_amount": 100,
        "ultimate_parent_legal_enti": "Dave's Pizza LLC",
        "ultimate_parent_unique_ide": "123",
    }
    asst_trans_3 = {
        "pk": 4,
        "record_type": 444,
        "transaction": TransactionNormalized.objects.get(pk=4),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
        "cfda_number": "10.002",
        "cfda_title": "CFDA Title 2",
        "federal_action_obligation": 400,
        "funding_office_name": "funding_office",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": "Charlotte",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_name": "North Carolina",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "modified_at": "2000-01-02T00:00:00Z",
        "non_federal_funding_amount": 0,
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "place_of_perfor_state_code": "TX",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_co": "023",
        "place_of_perform_county_na": "Tripoli",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_city": "Austin",
        "place_of_performance_congr": "-0-",
        "place_of_performance_forei": None,
        "place_of_performance_zip5": "40221",
        "total_funding_amount": 400,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
    }
    asst_trans_4 = {
        "pk": 5,
        "record_type": 555,
        "transaction": TransactionNormalized.objects.get(pk=5),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
        "cfda_number": "10.002",
        "cfda_title": "CFDA Title 2",
        "federal_action_obligation": 100,
        "funding_office_name": "funding_office",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": "Charlotte",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "USA",
        "legal_entity_country_name": "UNITED STATES",
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_name": "North Carolina",
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "modified_at": "2000-01-02T00:00:00Z",
        "non_federal_funding_amount": 0,
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "place_of_perfor_state_code": "TX",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_co": "023",
        "place_of_perform_county_na": "Tripoli",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_city": "Austin",
        "place_of_performance_congr": "-0-",
        "place_of_performance_forei": None,
        "place_of_performance_zip5": "40221",
        "total_funding_amount": 100,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
    }

    asst_trans_5 = {
        "pk": 13,
        "record_type": 666,
        "transaction": TransactionNormalized.objects.get(pk=13),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
        "cfda_number": "10.002",
        "cfda_title": "CFDA Title 2",
        "federal_action_obligation": 100,
        "funding_office_name": "funding_office",
        "legal_entity_address_line1": "123 main st",
        "legal_entity_address_line2": None,
        "legal_entity_address_line3": None,
        "legal_entity_city_name": None,
        "legal_entity_foreign_city": "Ontario",
        "legal_entity_congressional": "90",
        "legal_entity_country_code": "CAN",
        "legal_entity_country_name": "CANADA",
        "legal_entity_county_code": "019",
        "legal_entity_county_name": None,
        "legal_entity_state_code": "ONT",
        "legal_entity_state_name": None,
        "legal_entity_zip_last4": "5312",
        "legal_entity_zip5": "12204",
        "modified_at": "2000-01-02T00:00:00Z",
        "non_federal_funding_amount": 0,
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "place_of_perfor_state_code": "TX",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_country_n": "Pacific Delta Amazon",
        "place_of_perform_county_co": "023",
        "place_of_perform_county_na": "Tripoli",
        "place_of_perform_zip_last4": "2135",
        "place_of_performance_city": "Austin",
        "place_of_performance_congr": "-0-",
        "place_of_performance_forei": None,
        "place_of_performance_zip5": "40221",
        "total_funding_amount": 100,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
    }

    mommy.make("awards.TransactionFABS", **asst_trans_1)
    mommy.make("awards.TransactionFABS", **asst_trans_2)
    mommy.make("awards.TransactionFABS", **asst_trans_3)
    mommy.make("awards.TransactionFABS", **asst_trans_4)
    mommy.make("awards.TransactionFABS", **asst_trans_5)

    # Transaction FPDS
    cont_trans_1 = {
        "pk": 2,
        "transaction": TransactionNormalized.objects.get(pk=2),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
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
        "funding_office_name": "funding_office",
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
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_descrip": "North Carolina",
        "legal_entity_zip5": "12204",
        "legal_entity_zip_last4": "5312",
        "major_program": None,
        "materials_supplies_descrip": "NO",
        "multi_year_contract_desc": "NO",
        "multiple_or_single_aw_desc": None,
        "naics": "111120",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "other_than_full_and_o_desc": None,
        "place_of_perf_country_desc": "UNITED STATES",
        "place_of_perform_city_name": "Charlotte",
        "place_of_perform_country_c": "USA",
        "place_of_perform_county_co": "019",
        "place_of_perform_county_na": "BUNCOMBE",
        "place_of_performance_congr": "90",
        "place_of_performance_state": "NC",
        "place_of_perform_zip_last4": "5312",
        "place_of_performance_zip5": "12204",
        "period_of_perf_potential_e": "2003-04-05",
        "price_evaluation_adjustmen": None,
        "product_or_service_co_desc": None,
        "product_or_service_code": "1005",
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
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "type_of_idc_description": None,
        "type_set_aside_description": None,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_2 = {
        "pk": 6,
        "transaction": TransactionNormalized.objects.get(pk=6),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
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
        "funding_office_name": "funding_office",
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
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_descrip": "North Carolina",
        "legal_entity_zip5": "12204",
        "legal_entity_zip_last4": "5312",
        "major_program": None,
        "materials_supplies_descrip": "NO",
        "multi_year_contract_desc": "NO",
        "multiple_or_single_aw_desc": None,
        "naics": "111120",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "other_than_full_and_o_desc": None,
        "place_of_perf_country_desc": "UNITED STATES",
        "place_of_perform_city_name": "Charlotte",
        "place_of_perform_country_c": "USA",
        "place_of_perform_county_co": "019",
        "place_of_perform_county_na": "BUNCOMBE",
        "place_of_performance_congr": "90",
        "place_of_performance_state": "NC",
        "place_of_perform_zip_last4": "5312",
        "place_of_performance_zip5": "12204",
        "period_of_perf_potential_e": "2003-04-05",
        "price_evaluation_adjustmen": None,
        "product_or_service_co_desc": None,
        "product_or_service_code": "A136",
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
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "type_of_idc_description": None,
        "type_set_aside_description": None,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_3 = {
        "pk": 7,
        "transaction": TransactionNormalized.objects.get(pk=7),
        "awardee_or_recipient_legal": "LEGAL ENTITY",
        "awardee_or_recipient_uniqu": "456",
        "awarding_office_name": "awarding_office",
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
        "funding_office_name": "funding_office",
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
        "legal_entity_county_code": "019",
        "legal_entity_county_name": "BUNCOMBE",
        "legal_entity_state_code": "NC",
        "legal_entity_state_descrip": "North Carolina",
        "legal_entity_zip5": "12204",
        "legal_entity_zip_last4": "5312",
        "major_program": None,
        "materials_supplies_descrip": "NO",
        "multi_year_contract_desc": "NO",
        "multiple_or_single_aw_desc": None,
        "naics": "111120",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "other_than_full_and_o_desc": None,
        "place_of_perf_country_desc": "UNITED STATES",
        "place_of_perform_city_name": "Charlotte",
        "place_of_perform_country_c": "USA",
        "place_of_perform_county_co": "019",
        "place_of_perform_county_na": "BUNCOMBE",
        "place_of_performance_congr": "90",
        "place_of_performance_state": "NC",
        "place_of_perform_zip_last4": "5312",
        "place_of_performance_zip5": "12204",
        "period_of_perf_potential_e": "2003-04-05",
        "price_evaluation_adjustmen": None,
        "product_or_service_co_desc": None,
        "product_or_service_code": "M123",
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
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "type_of_idc_description": None,
        "type_set_aside_description": None,
        "ultimate_parent_legal_enti": "PARENT LEGAL ENTITY",
        "ultimate_parent_unique_ide": "123",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_4 = {
        "pk": 8,
        "transaction": TransactionNormalized.objects.get(pk=8),
        "agency_id": "1000",
        "idv_type_description": None,
        "multiple_or_single_aw_desc": None,
        "piid": "AWARD7",
        "type_of_idc_description": None,
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_5 = {
        "pk": 9,
        "transaction": TransactionNormalized.objects.get(pk=9),
        "agency_id": "1000",
        "idv_type_description": "TYPE DESCRIPTION TRANS 9",
        "multiple_or_single_aw_desc": "AW DESCRIPTION TRANS 9",
        "piid": "AWARD8",
        "type_of_idc_description": "IDC DESCRIPTION TRANS 9",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_6 = {
        "pk": 10,
        "transaction": TransactionNormalized.objects.get(pk=10),
        "agency_id": "1000",
        "idv_type_description": "TYPE DESCRIPTION TRANS 10",
        "multiple_or_single_aw_desc": "AW DESCRIPTION TRANS 10",
        "piid": "AWARD9",
        "type_of_idc_description": "IDC DESCRIPTION TRANS 10",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }
    cont_trans_7 = {
        "pk": 11,
        "transaction": TransactionNormalized.objects.get(pk=11),
        "agency_id": "2000",
        "piid": "AWARD10",
        "legal_entity_zip4": "0000",
        "national_interest_action": "NONE",
        "national_interest_desc": "NONE",
    }

    mommy.make("awards.TransactionFPDS", **cont_trans_1)
    mommy.make("awards.TransactionFPDS", **cont_trans_2)
    mommy.make("awards.TransactionFPDS", **cont_trans_3)
    mommy.make("awards.TransactionFPDS", **cont_trans_4)
    mommy.make("awards.TransactionFPDS", **cont_trans_5)
    mommy.make("awards.TransactionFPDS", **cont_trans_6)
    mommy.make("awards.TransactionFPDS", **cont_trans_7)

    # Awards
    award_1 = {
        "pk": 1,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=1),
        "category": "grant",
        "date_signed": "2005-04-03",
        "description": "lorem ipsum",
        "generated_unique_award_id": "ASST_AGG_1830212.0481163_3620",
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "subaward_count": 10,
        "total_subaward_amount": 12345.00,
        "type": "11",
        "type_description": "OTHER FINANCIAL ASSISTA1NCE",
        "uri": 1234,
    }
    award_2 = {
        "pk": 2,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=2),
        "base_and_all_options_value": 2000,
        "category": "contract",
        "date_signed": "2004-03-02",
        "description": "lorem ipsum",
        "fpds_parent_agency_id": "9700",
        "generated_unique_award_id": "CONT_AWD_03VD_9700_SPM30012D3486_9700",
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "parent_award_piid": "1234",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "piid": "5678",
        "subaward_count": 10,
        "total_obligation": 1000,
        "total_subaward_amount": 12345.00,
        "type": "A",
        "type_description": "DEFINITIVE CONTRACT",
    }
    award_3 = {
        "pk": 3,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=3),
        "base_and_all_options_value": 600,
        "category": "grant",
        "date_signed": "2004-03-02",
        "description": "lorem ipsum",
        "generated_unique_award_id": "",
        "parent_award_piid": "0001",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "piid": "0001",
        "subaward_count": 0,
        "total_obligation": 600,
        "total_subaward_amount": 0,
        "type": "03",
        "type_description": "FORMULA GRANT (A)",
    }
    award_4 = {
        "pk": 4,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=3),
        "base_and_all_options_value": 600,
        "category": "idv",
        "date_signed": "2004-03-02",
        "description": "lorem ipsum",
        "generated_unique_award_id": "",
        "parent_award_piid": None,
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "piid": "1234",
        "subaward_count": 0,
        "total_obligation": 600,
        "total_subaward_amount": 0,
        "type": "IDV_B",
        "type_description": "INDEFINITE DELIVERY CONTRACT",
    }
    award_5 = {
        "pk": 5,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=6),
        "base_and_all_options_value": 2000,
        "category": "contract",
        "date_signed": "2004-03-02",
        "description": "lorem ipsum",
        "fpds_parent_agency_id": "9700",
        "generated_unique_award_id": "CONT_AWD_WHATEVER",
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "parent_award_piid": "1234",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "piid": "5678",
        "subaward_count": 10,
        "total_obligation": 1000,
        "total_subaward_amount": 12345.00,
        "type": "A",
        "type_description": "DEFINITIVE CONTRACT",
    }
    award_6 = {
        "pk": 6,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=7),
        "base_and_all_options_value": 2000,
        "category": "contract",
        "date_signed": "2004-03-02",
        "description": "lorem ipsum",
        "fpds_parent_agency_id": "9700",
        "generated_unique_award_id": "CONT_AWD_WHATEVER2",
        "officer_1_amount": 10000.00,
        "officer_1_name": "Tom",
        "officer_2_amount": 1234.00,
        "officer_2_name": "Stan Burger",
        "parent_award_piid": "1234",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "piid": "5678",
        "subaward_count": 10,
        "total_obligation": 1000,
        "total_subaward_amount": 12345.00,
        "type": "A",
        "type_description": "DEFINITIVE CONTRACT",
    }
    award_7 = {
        "pk": 7,
        "latest_transaction": TransactionNormalized.objects.get(pk=8),
        "category": "contract",
        "fpds_parent_agency_id": 1000,
        "generated_unique_award_id": "CONT_AWD_AWARD7_1000_AWARD8_1000",
        "parent_award_piid": "AWARD8",
        "piid": "AWARD7",
        "type": "A",
    }
    award_8 = {
        "pk": 8,
        "latest_transaction": TransactionNormalized.objects.get(pk=9),
        "category": "idv",
        "fpds_parent_agency_id": 1000,
        "generated_unique_award_id": "CONT_IDV_AWARD8_1000",
        "parent_award_piid": "AWARD9",
        "piid": "AWARD8",
        "type": "IDV_A",
    }
    award_9 = {
        "pk": 9,
        "latest_transaction": TransactionNormalized.objects.get(pk=10),
        "category": "idv",
        "fpds_parent_agency_id": None,
        "generated_unique_award_id": "CONT_IDV_AWARD9_1000",
        "parent_award_piid": None,
        "piid": "AWARD9",
        "type": "IDV_A",
    }
    award_10 = {
        "pk": 10,
        "latest_transaction": TransactionNormalized.objects.get(pk=11),
        "category": "contract",
        "fpds_parent_agency_id": None,
        "generated_unique_award_id": "CONT_AWD_AWARD10_2000",
        "parent_award_piid": None,
        "piid": "AWARD10",
        "type": "A",
    }
    award_11 = {
        "pk": 11,
        "type": "03",
        "type_description": "FORMULA GRANT (A)",
        "category": "grant",
        "piid": "000",
        "parent_award_piid": "000",
        "description": "lorem ipsum",
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "total_obligation": 600,
        "base_and_all_options_value": 600,
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
        "generated_unique_award_id": "ASST_NON_:~$@*\"()#/,^&+=`!'%/_. -_9700",
        "latest_transaction": TransactionNormalized.objects.get(pk=3),
        "total_subaward_amount": 0,
        "subaward_count": 0,
        "date_signed": "2004-03-02",
    }
    award_13 = {
        "pk": 13,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=13),
        "category": "grant",
        "date_signed": "2005-04-03",
        "description": "lorem ipsum",
        "generated_unique_award_id": "ASST_AGG_whatever_3620",
        "officer_1_amount": 50000.00,
        "officer_1_name": "John Apple",
        "officer_2_amount": 4623.00,
        "officer_2_name": "Wally World",
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
        "subaward_count": 10,
        "total_subaward_amount": 12345.00,
        "type": "11",
        "type_description": "OTHER FINANCIAL ASSISTA1NCE",
        "uri": 1234,
    }

    mommy.make("awards.Award", **award_1)
    mommy.make("awards.Award", **award_2)
    mommy.make("awards.Award", **award_3)
    mommy.make("awards.Award", **award_4)
    mommy.make("awards.Award", **award_5)
    mommy.make("awards.Award", **award_6)
    mommy.make("awards.Award", **award_7)
    mommy.make("awards.Award", **award_8)
    mommy.make("awards.Award", **award_9)
    mommy.make("awards.Award", **award_10)
    mommy.make("awards.Award", **award_11)
    mommy.make("awards.Award", **award_13)

    # Parent Award
    parent_award_1 = {
        "award_id": 4,
        "direct_base_and_all_options_value": 12,
        "direct_base_exercised_options_val": 0,
        "direct_contract_count": 1,
        "direct_idv_count": 0,
        "direct_total_obligation": 4500,
        "generated_unique_award_id": "CONT_IDV_1234_9700",
        "rollup_base_and_all_options_value": 12,
        "rollup_base_exercised_options_val": 0,
        "rollup_contract_count": 1,
        "rollup_idv_count": 0,
        "rollup_total_obligation": 4500,
        "parent_award_id": None,
    }
    parent_award_2 = {"award_id": 8, "generated_unique_award_id": "CONT_IDV_AWARD8_1000", "parent_award_id": 9}
    parent_award_3 = {"award_id": 9, "generated_unique_award_id": "CONT_IDV_AWARD9_1000", "parent_award_id": None}

    mommy.make("awards.ParentAward", **parent_award_1)
    mommy.make("awards.ParentAward", **parent_award_2)
    mommy.make("awards.ParentAward", **parent_award_3)


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


def test_award_endpoint_parent_award(client, awards_and_transactions):
    # Test contract award with parent
    resp = client.get("/api/v2/awards/7/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["parent_award"] == expected_contract_award_parent

    # Test contract award without parent
    resp = client.get("/api/v2/awards/10/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["parent_award"] is None

    # Test idv award with parent
    resp = client.get("/api/v2/awards/8/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["parent_award"] == expected_idv_award_parent

    # Test idv award without parent
    resp = client.get("/api/v2/awards/9/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["parent_award"] is None


def test_award_mulitple_cfdas(client, awards_and_transactions):

    resp = client.get("/api/v2/awards/3/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["cfda_info"] == [
        {
            "applicant_eligibility": None,
            "beneficiary_eligibility": None,
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
            "applicant_eligibility": "Hello. I am an applicant eligibility statement.",
            "beneficiary_eligibility": "Hello. I am a beneficiary eligibility statement.",
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


def test_award_psc_hierarchy_types(client, awards_and_transactions):
    resp = client.get("/api/v2/awards/5/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["psc_hierarchy"] == {
        "toptier_code": {"description": "R&D", "code": "A"},
        "midtier_code": {"description": "R&D - Steak Sauce", "code": "A1"},
        "subtier_code": {"description": "R&D - Brand specific steak condiments", "code": "A13"},
        "base_code": {"description": "R&D - Very specific steak research", "code": "A136"},
    }

    resp = client.get("/api/v2/awards/6/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["psc_hierarchy"] == {
        "toptier_code": {"description": "Something", "code": "M"},
        "midtier_code": {"description": "Something More Specific", "code": "M1"},
        "subtier_code": {},
        "base_code": {"description": "Something Most Specific", "code": "M123"},
    }


def test_foreign_city(client, awards_and_transactions):
    resp = client.get("/api/v2/awards/13/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["recipient"]["location"] == {
        "address_line1": "123 main st",
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Ontario",
        "county_code": "019",
        "county_name": None,
        "state_code": "ONT",
        "state_name": None,
        "zip5": "12204",
        "zip4": "5312",
        "foreign_postal_code": None,
        "country_name": "CANADA",
        "location_country_code": "CAN",
        "congressional_code": "90",
    }


def test_special_characters(client, awards_and_transactions):
    resp = client.get("/api/v2/awards/ASST_NON_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
    assert resp.status_code == status.HTTP_200_OK

    resp = client.get("/api/v2/awards/count/transaction/ASST_NON_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
    assert resp.status_code == status.HTTP_200_OK
    resp = client.get("/api/v2/awards/count/subaward/ASST_NON_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
    assert resp.status_code == status.HTTP_200_OK
    resp = client.get("/api/v2/awards/count/federal_account/ASST_NON_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
    assert resp.status_code == status.HTTP_200_OK


def test_zip4_switch(client, awards_and_transactions):
    resp = client.get("/api/v2/awards/10/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["recipient"]["location"]["zip4"] == "0000"


def test_file_c_data(client, awards_and_transactions):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2019,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_reveal_date="2020-04-01",
        period_start_date="2020-04-01",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2018,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_reveal_date="2020-04-01",
        period_start_date="2020-04-01",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_reveal_date="2020-04-01",
        period_start_date="2020-04-01",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        pk=2,
        reporting_fiscal_period=8,
        reporting_fiscal_year=2019,
        reporting_period_end="2019-06-30",
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="2020-04-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        transaction_obligated_amount=100,
        gross_outlay_amount_by_award_cpe=100,
        disaster_emergency_fund=defc,
        submission_id=2,
    )
    # fiscal period is not 12 & is not after 2020-04-01, so we expect no data to come back
    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["account_obligations_by_defc"] == [{"code": "L", "amount": 100.0}]
    assert json.loads(resp.content.decode("utf-8"))["account_outlays_by_defc"] == [{"code": "L", "amount": 0.0}]
    assert json.loads(resp.content.decode("utf-8"))["total_account_obligation"] == 100.0
    assert json.loads(resp.content.decode("utf-8"))["total_account_outlay"] == 0.0
    mommy.make(
        "submissions.SubmissionAttributes",
        pk=1,
        reporting_fiscal_period=12,
        reporting_fiscal_year=2020,
        reporting_period_end="2020-06-30",
        quarter_format_flag=True,
        is_final_balances_for_fy=True,
        reporting_period_start="2020-04-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        transaction_obligated_amount=100,
        gross_outlay_amount_by_award_cpe=100,
        disaster_emergency_fund=defc,
        submission_id=1,
    )
    resp = client.get("/api/v2/awards/1/")
    # now we have the period 12 data, so we expect outlays here
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["account_obligations_by_defc"] == [{"code": "L", "amount": 200.0}]
    assert json.loads(resp.content.decode("utf-8"))["account_outlays_by_defc"] == [{"code": "L", "amount": 100.0}]
    assert json.loads(resp.content.decode("utf-8"))["total_account_obligation"] == 200.0
    assert json.loads(resp.content.decode("utf-8"))["total_account_outlay"] == 100.0
    mommy.make(
        "submissions.SubmissionAttributes",
        pk=3,
        reporting_fiscal_period=10,
        reporting_fiscal_year=2018,
        reporting_period_end="2019-06-30",
        quarter_format_flag=False,
        is_final_balances_for_fy=False,
        reporting_period_start="2020-04-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        transaction_obligated_amount=10,
        gross_outlay_amount_by_award_cpe=10,
        disaster_emergency_fund=defc,
        submission_id=3,
    )
    # again, period is not 12, no data reported
    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["account_obligations_by_defc"] == [{"code": "L", "amount": 210.0}]
    assert json.loads(resp.content.decode("utf-8"))["account_outlays_by_defc"] == [{"code": "L", "amount": 100.0}]
    assert json.loads(resp.content.decode("utf-8"))["total_account_obligation"] == 210.0
    assert json.loads(resp.content.decode("utf-8"))["total_account_outlay"] == 100.0
    mommy.make(
        "submissions.SubmissionAttributes",
        pk=4,
        reporting_fiscal_period=12,
        reporting_fiscal_year=2018,
        reporting_period_end="2020-06-30",
        quarter_format_flag=True,
        is_final_balances_for_fy=True,
        reporting_period_start="2020-04-01",
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        transaction_obligated_amount=10,
        gross_outlay_amount_by_award_cpe=10,
        disaster_emergency_fund=defc,
        submission_id=4,
    )
    # expect outlays here
    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["account_obligations_by_defc"] == [{"code": "L", "amount": 220.0}]
    assert json.loads(resp.content.decode("utf-8"))["account_outlays_by_defc"] == [{"code": "L", "amount": 110.0}]
    assert json.loads(resp.content.decode("utf-8"))["total_account_obligation"] == 220.0
    assert json.loads(resp.content.decode("utf-8"))["total_account_outlay"] == 110.0
    mommy.make(
        "submissions.SubmissionAttributes",
        pk=5,
        reporting_fiscal_period=12,
        reporting_fiscal_year=2019,
        is_final_balances_for_fy=True,
        reporting_period_end="2020-06-30",
        quarter_format_flag=True,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        award_id=1,
        transaction_obligated_amount=0,
        gross_outlay_amount_by_award_cpe=0,
        disaster_emergency_fund=defc,
        submission_id=5,
    )
    # period is 12 but amounts are 0, so we expect no change
    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["account_obligations_by_defc"] == [{"code": "L", "amount": 220.0}]
    assert json.loads(resp.content.decode("utf-8"))["account_outlays_by_defc"] == [{"code": "L", "amount": 110.0}]
    assert json.loads(resp.content.decode("utf-8"))["total_account_obligation"] == 220.0
    assert json.loads(resp.content.decode("utf-8"))["total_account_outlay"] == 110.0


expected_response_asst = {
    "id": 1,
    "record_type": 111,
    "type": "11",
    "category": "grant",
    "type_description": "OTHER FINANCIAL ASSISTA1NCE",
    "uri": "1234",
    "fain": None,
    "generated_unique_award_id": "ASST_AGG_1830212.0481163_3620",
    "description": "lorem ipsum",
    "cfda_info": [
        {
            "applicant_eligibility": None,
            "beneficiary_eligibility": None,
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
        "has_agency_page": False,
        "toptier_agency": {"name": "TOPTIER AGENCY 1", "abbreviation": "TA1", "code": "ABC"},
        "subtier_agency": {"name": "SUBTIER AGENCY 1", "abbreviation": "SA1", "code": "DEF"},
        "office_agency_name": "awarding_office",
    },
    "funding_agency": {
        "id": 1,
        "has_agency_page": False,
        "toptier_agency": {"name": "TOPTIER AGENCY 1", "abbreviation": "TA1", "code": "ABC"},
        "subtier_agency": {"name": "SUBTIER AGENCY 1", "abbreviation": "SA1", "code": "DEF"},
        "office_agency_name": "funding_office",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367-C",
        "recipient_name": "LEGAL ENTITY",
        "recipient_unique_id": "456",
        "parent_recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P",
        "parent_recipient_name": "PARENT LEGAL ENTITY",
        "parent_recipient_unique_id": "123",
        "business_categories": ["Small Business"],
        "location": {
            "address_line1": "123 main st",
            "address_line2": None,
            "address_line3": None,
            "foreign_province": None,
            "city_name": "Charlotte",
            "county_code": "019",
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
    },
    "subaward_count": 10,
    "total_subaward_amount": 12345.0,
    "executive_details": {
        "officers": [
            {"name": "John Apple", "amount": 50000.00},
            {"name": "Wally World", "amount": 4623.00},
            {"name": None, "amount": None},
            {"name": None, "amount": None},
            {"name": None, "amount": None},
        ]
    },
    "period_of_performance": {"start_date": "2004-02-04", "end_date": "2005-02-04", "last_modified_date": "2000-01-02"},
    "place_of_performance": {
        "address_line1": None,
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Austin",
        "county_code": "023",
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
    "account_obligations_by_defc": [],
    "account_outlays_by_defc": [],
    "total_account_obligation": 0,
    "total_account_outlay": 0,
}


expected_response_cont = {
    "id": 2,
    "generated_unique_award_id": "CONT_AWD_03VD_9700_SPM30012D3486_9700",
    "type": "A",
    "category": "contract",
    "type_description": "DEFINITIVE CONTRACT",
    "piid": "5678",
    "description": "lorem ipsum",
    "awarding_agency": {
        "id": 1,
        "has_agency_page": False,
        "toptier_agency": {"name": "TOPTIER AGENCY 1", "abbreviation": "TA1", "code": "ABC"},
        "subtier_agency": {"name": "SUBTIER AGENCY 1", "abbreviation": "SA1", "code": "DEF"},
        "office_agency_name": "awarding_office",
    },
    "funding_agency": {
        "id": 1,
        "has_agency_page": False,
        "toptier_agency": {"name": "TOPTIER AGENCY 1", "abbreviation": "TA1", "code": "ABC"},
        "subtier_agency": {"name": "SUBTIER AGENCY 1", "abbreviation": "SA1", "code": "DEF"},
        "office_agency_name": "funding_office",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367-C",
        "recipient_name": "LEGAL ENTITY",
        "recipient_unique_id": "456",
        "parent_recipient_hash": "8ec6b128-58cf-3ee5-80bb-e749381dfcdc-P",
        "parent_recipient_name": "PARENT LEGAL ENTITY",
        "parent_recipient_unique_id": "123",
        "business_categories": ["Small Business"],
        "location": {
            "address_line1": "123 main st",
            "address_line2": None,
            "address_line3": None,
            "foreign_province": None,
            "city_name": "Charlotte",
            "county_code": "019",
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
        "county_code": "019",
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
        "naics": "111120",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "national_interest_action": "NONE",
        "national_interest_action_description": "NONE",
        "number_of_offers_received": None,
        "other_than_full_and_open": None,
        "other_than_full_and_open_description": None,
        "price_evaluation_adjustment": None,
        "dod_acquisition_program": "000",
        "dod_acquisition_program_description": None,
        "information_technology_commercial_item_category": None,
        "information_technology_commercial_item_category_description": "NOT IT PRODUCTS OR SERVICES",
        "product_or_service_code": "1005",
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
        "officers": [
            {"name": "Tom", "amount": 10000.00},
            {"name": "Stan Burger", "amount": 1234.00},
            {"name": None, "amount": None},
            {"name": None, "amount": None},
            {"name": None, "amount": None},
        ]
    },
    "date_signed": "2004-03-02",
    "naics_hierarchy": {
        "toptier_code": {"description": "Agriculture", "code": "11"},
        "midtier_code": {"description": "Soybean & Oilseed Agriculture", "code": "1111"},
        "base_code": {"description": "Soybean Harvesting", "code": "111120"},
    },
    "psc_hierarchy": {
        "toptier_code": {},
        "midtier_code": {"description": "Whatever", "code": "10"},
        "subtier_code": {},
        "base_code": {"description": "More specific whatever", "code": "1005"},
    },
    "parent_award": {
        "agency_id": None,
        "agency_name": None,
        "sub_agency_id": None,
        "sub_agency_name": None,
        "award_id": 4,
        "generated_unique_award_id": "CONT_IDV_1234_9700",
        "idv_type_description": None,
        "multiple_or_single_aw_desc": None,
        "piid": None,
        "type_of_idc_description": None,
    },
    "account_obligations_by_defc": [],
    "account_outlays_by_defc": [],
    "total_account_obligation": 0,
    "total_account_outlay": 0,
}

expected_contract_award_parent = {
    "agency_id": 2,
    "agency_name": "TOPTIER AGENCY 2",
    "sub_agency_id": "1000",
    "sub_agency_name": "SUBTIER AGENCY 2",
    "award_id": 8,
    "generated_unique_award_id": "CONT_IDV_AWARD8_1000",
    "idv_type_description": "TYPE DESCRIPTION TRANS 9",
    "multiple_or_single_aw_desc": "AW DESCRIPTION TRANS 9",
    "piid": "AWARD8",
    "type_of_idc_description": "IDC DESCRIPTION TRANS 9",
}

expected_idv_award_parent = {
    "agency_id": 2,
    "agency_name": "TOPTIER AGENCY 2",
    "sub_agency_id": "1000",
    "sub_agency_name": "SUBTIER AGENCY 2",
    "award_id": 9,
    "generated_unique_award_id": "CONT_IDV_AWARD9_1000",
    "idv_type_description": "TYPE DESCRIPTION TRANS 10",
    "multiple_or_single_aw_desc": "AW DESCRIPTION TRANS 10",
    "piid": "AWARD9",
    "type_of_idc_description": "IDC DESCRIPTION TRANS 10",
}
