import pytest
from model_bakery import baker
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.references.models import ToptierAgency, SubtierAgency


@pytest.fixture
def set_up_related_award_objects(db):

    subag = {"pk": 1, "name": "agency name", "abbreviation": "some other stuff"}

    baker.make("recipient.DUNS", legal_business_name="Sams Club", _fill_optional=True)
    baker.make("references.SubtierAgency", **subag, _fill_optional=True)
    baker.make("references.ToptierAgency", **subag, _fill_optional=True)

    ag = {"pk": 1, "toptier_agency": ToptierAgency.objects.get(pk=1), "subtier_agency": SubtierAgency.objects.get(pk=1)}

    baker.make("references.Agency", **ag, _fill_optional=True)
    cont_data = {
        "pk": 1,
        "transaction_id": 1,
        "is_fpds": True,
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "naics_code": "333911",
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
        "business_categories": ["small_business"],
    }
    baker.make("search.TransactionSearch", **cont_data)


def create_tree(awards):
    generic_model = {
        "type_description": "FAKE",
        "category": "contract",
        "description": "lorem ipsum",
        "awarding_agency_id": 1,
        "funding_agency_id": 1,
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
        "latest_transaction": TransactionNormalized.objects.get(pk=1),
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
    }
    award_dict = []
    for award in awards:
        new_model = {**generic_model, **award}
        award_dict.append(new_model)
    return award_dict
