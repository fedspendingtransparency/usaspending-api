import datetime
import pytest
import json

from rest_framework import status
from model_mommy import mommy

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.references.models import Agency, Location, ToptierAgency, SubtierAgency, OfficeAgency, LegalEntity


@pytest.fixture
def awards_and_transactions(db):
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
    subag = {"pk": 1, "name": "agency name", "abbreviation": "some other stuff"}

    trans_asst = {"pk": 1}
    trans_cont = {"pk": 2}
    duns = {"awardee_or_recipient_uniqu": 123, "legal_business_name": "Sams Club"}
    mommy.make("references.Cfda", program_number=1234)
    mommy.make("references.Location", **loc)
    mommy.make("recipient.DUNS", **duns)
    mommy.make("references.SubtierAgency", **subag)
    mommy.make("references.ToptierAgency", **subag)
    mommy.make("references.OfficeAgency", name="office_agency", office_agency_id=1)

    le = {
        "pk": 1,
        "business_categories": ["small_business"],
        # "location": Location.objects.get(pk=1),
    }

    ag = {
        "pk": 1,
        "toptier_agency": ToptierAgency.objects.get(pk=1),
        "subtier_agency": SubtierAgency.objects.get(pk=1),
        "office_agency": OfficeAgency.objects.get(pk=1),
    }

    mommy.make("awards.TransactionNormalized", **trans_asst)
    mommy.make("awards.TransactionNormalized", **trans_cont)
    mommy.make("references.Agency", **ag)
    mommy.make("references.LegalEntity", **le)

    asst_data = {
        "pk": 1,
        "transaction": TransactionNormalized.objects.get(pk=1),
        "cfda_number": 1234,
        "cfda_title": "Shiloh",
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
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
    }
    cont_data = {
        "pk": 2,
        "transaction": TransactionNormalized.objects.get(pk=2),
        "idv_type_description": "",
        "type_of_idc_description": "",
        "referenced_idv_agency_iden": "",
        "multiple_or_single_aw_desc": "",
        "solicitation_identifier": "",
        "solicitation_procedures": "",
        "number_of_offers_received": "",
        "extent_competed": "",
        "other_than_full_and_o_desc": "",
        "type_set_aside_description": "",
        "commercial_item_acquisitio": "",
        "commercial_item_test_desc": "",
        "evaluated_preference_desc": "",
        "fed_biz_opps_description": "",
        "small_business_competitive": "",
        "fair_opportunity_limi_desc": "",
        "product_or_service_code": "",
        "product_or_service_co_desc": None,
        "naics": "",
        "dod_claimant_program_code": "",
        "program_system_or_equipmen": "",
        "information_technolog_desc": "",
        "sea_transportation_desc": "",
        "clinger_cohen_act_pla_desc": "",
        "construction_wage_rat_desc": "",
        "labor_standards_descrip": "",
        "materials_supplies_descrip": "",
        "cost_or_pricing_data_desc": "",
        "domestic_or_foreign_e_desc": "",
        "foreign_funding_desc": "",
        "interagency_contract_desc": "",
        "major_program": "",
        "price_evaluation_adjustmen": "",
        "program_acronym": "",
        "subcontracting_plan": "",
        "multi_year_contract_desc": "",
        "purchase_card_as_paym_desc": "",
        "consolidated_contract_desc": "",
        "type_of_contract_pric_desc": "",
        "awardee_or_recipient_legal": "John's Pizza",
        "awardee_or_recipient_uniqu": "456",
        "ultimate_parent_legal_enti": None,
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
        "place_of_perform_country_c": "USA",
        "place_of_perf_country_desc": "UNITED STATES",
        "place_of_performance_state": "NC",
        "place_of_perform_city_name": "Charlotte",
        "place_of_perform_county_na": "BUNCOMBE",
        "place_of_performance_zip4a": "5312",
        "place_of_performance_congr": "90",
        "place_of_performance_zip5": "12204",
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "naics": "333911",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "referenced_idv_agency_iden": "9700",
        "idv_type_description": None,
        "multiple_or_single_aw_desc": None,
        "type_of_idc_description": None,
        "dod_claimant_program_code": "C9E",
        "clinger_cohen_act_pla_desc": "NO",
        "commercial_item_acquisitio": "A",
        "commercial_item_test_desc": "NO",
        "consolidated_contract_desc": "NOT CONSOLIDATED",
        "cost_or_pricing_data_desc": "NO",
        "construction_wage_rat_desc": "NO",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "extent_competed": "D",
        "fed_biz_opps_description": "YES",
        "foreign_funding_desc": "NOT APPLICABLE",
        "information_technolog_desc": "NOT IT PRODUCTS OR SERVICES",
        "interagency_contract_desc": "NOT APPLICABLE",
        "major_program": None,
        "purchase_card_as_paym_desc": "NO",
        "multi_year_contract_desc": "NO",
        "number_of_offers_received": None,
        "price_evaluation_adjustmen": None,
        "product_or_service_code": "4730",
        "program_acronym": None,
        "other_than_full_and_o_desc": None,
        "sea_transportation_desc": "NO",
        "labor_standards_descrip": "NO",
        "small_business_competitive": "False",
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "fair_opportunity_limi_desc": None,
        "subcontracting_plan": "B",
        "program_system_or_equipmen": "000",
        "type_set_aside_description": None,
        "materials_supplies_descrip": "NO",
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
    }
    mommy.make("awards.TransactionFABS", **asst_data)
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
        "generated_unique_award_id": "ASST_AW_3620_-NONE-_1830212.0481163",
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "recipient": LegalEntity.objects.get(pk=1),
        "place_of_performance": Location.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=1),
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
        "generated_unique_award_id": "CONT_AW_9700_9700_03VD_SPM30012D3486",
        "place_of_performance": Location.objects.get(pk=1),
        "latest_transaction": TransactionNormalized.objects.get(pk=2),
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
    }
    mommy.make("awards.Award", **award_1_model)
    mommy.make("awards.Award", **award_2_model)


@pytest.mark.django_db
def test_award_last_updated_endpoint(client):
    """Test the awards endpoint."""

    test_date = datetime.datetime.now()
    test_date_reformatted = test_date.strftime("%m/%d/%Y")

    mommy.make("awards.Award", update_date=test_date)
    mommy.make("awards.Award", update_date="")

    resp = client.get("/api/v2/awards/last_updated/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["last_updated"] == test_date_reformatted


@pytest.mark.django_db
def test_award_endpoint_generated_id(client, awards_and_transactions):

    resp = client.get("/api/v2/awards/ASST_AW_3620_-NONE-_1830212.0481163/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_asst

    resp = client.get("/api/v2/awards/CONT_AW_9700_9700_03VD_SPM30012D3486/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_cont

    resp = client.get("/api/v2/awards/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_asst

    resp = client.get("/api/v2/awards/2/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_cont


@pytest.mark.django_db
def test_idv_award_amount_endpoint(client):

    mommy.make("awards.Award", pk=1)
    mommy.make(
        "awards.ParentAward",
        award_id=1,
        generated_unique_award_id="CONT_AW_2",
        direct_idv_count=3,
        direct_contract_count=4,
        direct_total_obligation="5.01",
        direct_base_and_all_options_value="6.02",
        direct_base_exercised_options_val="7.03",
        rollup_idv_count=8,
        rollup_contract_count=9,
        rollup_total_obligation="10.04",
        rollup_base_and_all_options_value="11.05",
        rollup_base_exercised_options_val="12.06",
    )

    output_idv_amounts = {
        "award_id": 1,
        "generated_unique_award_id": "CONT_AW_2",
        "idv_count": 3,
        "contract_count": 4,
        "rollup_total_obligation": 10.04,
        "rollup_base_and_all_options_value": 11.05,
        "rollup_base_exercised_options_val": 12.06,
    }

    resp = client.get("/api/v2/awards/idvs/amounts/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == output_idv_amounts

    resp = client.get("/api/v2/awards/idvs/amounts/CONT_AW_2/")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == output_idv_amounts

    resp = client.get("/api/v2/awards/idvs/amounts/3/")
    assert resp.status_code == status.HTTP_404_NOT_FOUND
    assert json.loads(resp.content.decode("utf-8")) == {"detail": "No IDV award found with this id"}

    resp = client.get("/api/v2/awards/idvs/amounts/BOGUS_ID/")
    assert resp.status_code == status.HTTP_404_NOT_FOUND
    assert json.loads(resp.content.decode("utf-8")) == {"detail": "No IDV award found with this id"}

    resp = client.get("/api/v2/awards/idvs/amounts/INVALID_ID_&&&/")
    assert resp.status_code == status.HTTP_404_NOT_FOUND


expected_response_asst = {
    "id": 1,
    "type": "11",
    "category": "grant",
    "type_description": "OTHER FINANCIAL ASSISTANCE",
    "uri": "1234",
    "fain": None,
    "generated_unique_award_id": "ASST_AW_3620_-NONE-_1830212.0481163",
    "description": "lorem ipsum",
    "cfda_objectives": None,
    "cfda_number": "1234",
    "cfda_title": "Shiloh",
    "base_and_all_options_value": None,
    "base_exercised_options": None,
    "non_federal_funding": None,
    "total_funding": None,
    "total_loan_value": None,
    "total_obligation": None,
    "total_subsidy_cost": None,
    "awarding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "office_agency",
    },
    "funding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "office_agency",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367",
        "recipient_name": "John's Pizza",
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
        "business_categories": ["small_business"],
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
        "parent_recipient_name": None,
    },
    "subaward_count": 10,
    "total_subaward_amount": 12345.0,
    "period_of_performance": {
        "period_of_performance_current_end_date": "2005-02-04",
        "period_of_performance_start_date": "2004-02-04",
    },
    "place_of_performance": {
        "address_line1": None,
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Austin",
        "county_name": "Tripoli",
        "state_code": "TX",
        "zip5": "40221",
        "zip4": "2135",
        "foreign_postal_code": None,
        "country_name": "Pacific Delta Amazon",
        "location_country_code": "PDA",
        "congressional_code": "-0-",
    },
}

expected_response_cont = {
    "id": 2,
    "generated_unique_award_id": "CONT_AW_9700_9700_03VD_SPM30012D3486",
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
        "office_agency_name": "office_agency",
    },
    "funding_agency": {
        "id": 1,
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff", "code": None},
        "office_agency_name": "office_agency",
    },
    "recipient": {
        "recipient_hash": "f989e299-1f50-2600-f2f7-b6a45d11f367",
        "recipient_name": "John's Pizza",
        "recipient_unique_id": "456",
        "parent_recipient_unique_id": "123",
        "business_categories": ["small_business"],
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
        "parent_recipient_name": None,
    },
    "total_obligation": 1000.0,
    "base_and_all_options_value": 2000.0,
    "base_exercised_options_val": None,
    "period_of_performance": {
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
    },
    "place_of_performance": {
        "address_line1": None,
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
    "latest_transaction_contract_data": {
        "idv_type_description": None,
        "type_of_idc_description": None,
        "referenced_idv_agency_iden": "9700",
        "multiple_or_single_aw_desc": None,
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "number_of_offers_received": None,
        "extent_competed": "D",
        "other_than_full_and_o_desc": None,
        "type_set_aside_description": None,
        "commercial_item_acquisitio": "A",
        "commercial_item_test_desc": "NO",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "fed_biz_opps_description": "YES",
        "small_business_competitive": "False",
        "fair_opportunity_limi_desc": None,
        "product_or_service_code": "4730",
        "product_or_service_co_desc": None,
        "naics": "333911",
        "dod_claimant_program_code": "C9E",
        "program_system_or_equipmen": "000",
        "information_technolog_desc": "NOT IT PRODUCTS OR SERVICES",
        "sea_transportation_desc": "NO",
        "clinger_cohen_act_pla_desc": "NO",
        "construction_wage_rat_desc": "NO",
        "labor_standards_descrip": "NO",
        "materials_supplies_descrip": "NO",
        "cost_or_pricing_data_desc": "NO",
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
        "foreign_funding_desc": "NOT APPLICABLE",
        "interagency_contract_desc": "NOT APPLICABLE",
        "major_program": None,
        "price_evaluation_adjustmen": None,
        "program_acronym": None,
        "subcontracting_plan": "B",
        "multi_year_contract_desc": "NO",
        "purchase_card_as_paym_desc": "NO",
        "consolidated_contract_desc": "NOT CONSOLIDATED",
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
    },
    "subaward_count": 10,
    "total_subaward_amount": 12345.0,
    "executive_details": {"officers": []},
}
