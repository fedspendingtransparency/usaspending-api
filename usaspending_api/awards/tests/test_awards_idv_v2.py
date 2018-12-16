import datetime
import json
import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, Award
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
    mommy.make("references.OfficeAgency", name="office_agency")

    le = {
        "pk": 1,
        "recipient_name": "John's Pizza",
        "recipient_unique_id": 456,
        "parent_recipient_unique_id": 123,
        "business_categories": ["small_business"],
        "location": Location.objects.get(pk=1),
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
        "cfda_title": "farms",
    }

    latest_transaction_contract_data = {
        "agency_id": "192",
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
        "idv_type_description": "IDC",
        "information_technolog_desc": "NOT IT PRODUCTS OR SERVICES",
        "interagency_contract_desc": "NOT APPLICABLE",
        "labor_standards_descrip": "NO",
        "last_modified": "2018-08-24",
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
        "multiple_or_single_aw_desc": "MULTIPLE AWARD",
        "naics": "333911",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "number_of_offers_received": None,
        "ordering_period_end_date": "2025-06-30",
        "other_than_full_and_o_desc": None,
        "parent_award_id": "1",
        "period_of_performance_star": "2010-09-23",
        "piid": "0",
        "pk": 2,
        "place_of_perf_country_desc": "Pacific Delta Amazon",
        "place_of_perform_city_name": "Austin",
        "place_of_perform_country_c": "PDA",
        "place_of_perform_county_na": "Tripoli",
        "place_of_performance_congr": "-0-",
        "place_of_performance_state": "TX",
        "place_of_performance_zip4a": "2135",
        "place_of_performance_zip5": "40221",
        "price_evaluation_adjustmen": None,
        "product_or_service_code": "4730",
        "program_acronym": None,
        "program_system_or_equipmen": "000",
        "purchase_card_as_paym_desc": "NO",
        "referenced_idv_agency_iden": "168",
        "sea_transportation_desc": "NO",
        "small_business_competitive": "False",
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "subcontracting_plan": "B",
        "transaction": TransactionNormalized.objects.get(pk=2),
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        "type_set_aside_description": None,
        "ultimate_parent_legal_enti": None,
        "ultimate_parent_unique_ide": "123",
    }
    mommy.make("awards.TransactionFABS", **asst_data)
    mommy.make("awards.TransactionFPDS", **latest_transaction_contract_data)
    award_1_model = {
        "pk": 1,
        "type": "IDV_B_B",
        "category": "idv",
        "piid": 1234,
        "type_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        "description": "lorem ipsum",
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
        "type": "IDV_A",
        "type_description": "GWAC",
        "category": "idv",
        "piid": "5678",
        "parent_award_piid": "1234",
        "description": "lorem ipsum",
        "awarding_agency": Agency.objects.get(pk=1),
        "funding_agency": Agency.objects.get(pk=1),
        "recipient": LegalEntity.objects.get(pk=1),
        "total_obligation": 1000,
        "base_and_all_options_value": 2000,
        "generated_unique_award_id": "CONT_AW_9700_9700_03VD_SPM30012D3486",
        "latest_transaction": TransactionNormalized.objects.get(pk=2),
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
    }
    mommy.make("awards.Award", **award_1_model)
    mommy.make("awards.Award", **award_2_model)


@pytest.mark.django_db
def test_no_data_idv_award_endpoint(client):
    """Test the /v2/awards endpoint."""

    resp = client.get("/api/v2/awards/27254436/", content_type="application/json")
    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_null_awards():
    """Test the award.nonempty command."""
    mommy.make("awards.Award", total_obligation="2000", _quantity=2)
    mommy.make("awards.Award", type="U", total_obligation=None, date_signed=None, recipient=None)

    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2


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
    resp = client.get("/api/v2/awards/CONT_AW_9700_9700_03VD_SPM30012D3486/", content_type="application/json")
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_idv


expected_response_idv = {
    "id": 2,
    "type": "IDV_A",
    "parent_generated_unique_award_id": None,
    "generated_unique_award_id": "CONT_AW_9700_9700_03VD_SPM30012D3486",
    "category": "idv",
    "type_description": "GWAC",
    "piid": "5678",
    "parent_award_piid": "1234",
    "description": "lorem ipsum",
    "idv_dates": {"end_date": "2025-06-30", "last_modified_date": "2018-08-24", "start_date": None},
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
    "latest_transaction_contract_data": {
        "idv_type_description": "IDC",
        "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        "referenced_idv_agency_iden": "168",
        "multiple_or_single_aw_desc": "MULTIPLE AWARD",
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
