import datetime
import pytest
import json

from rest_framework import status
from model_mommy import mommy

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.references.models import Agency, Location, ToptierAgency, SubtierAgency, OfficeAgency, LegalEntity


@pytest.mark.django_db
def test_idv_award_endpoint(client):
    """Test the /v2/awards endpoint."""

    resp = client.get('/api/v2/awards/27254436')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 1

    assert client.get(
        '/api/v2/awards/27254436',
        content_type='application/json').status_code == status.HTTP_200_OK

    assert client.get(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "equals",
                "value": "0300"
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "combine_method": "OR",
                "filters": [{
                    "field": "funding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }, {
                    "field": "awarding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }]
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "ff",
                "value": "0300"
            }]
        })).status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_null_awards():
    """Test the award.nonempty command."""
    mommy.make('awards.Award', total_obligation="2000", _quantity=2)
    mommy.make(
        'awards.Award',
        type="U",
        total_obligation=None,
        date_signed=None,
        recipient=None)

    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2


@pytest.fixture
def awards_data(db):
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000)
    mommy.make(
        'awards.Award',
        piid='###',
        fain='ABC789',
        type='B',
        total_obligation=1000)
    mommy.make('awards.Award', fain='XYZ789', type='C', total_obligation=1000)


def test_award_total_grouped(client, awards_data):
    """Test award total endpoint with a group parameter."""

    resp = client.post(
        '/api/v1/awards/total/',
        content_type='application/json',
        data=json.dumps({
            'field': 'total_obligation',
            'group': 'type',
            'aggregate': 'sum'
        }))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    # our data has two different type codes, we should get two summarized items back
    assert len(results) == 2
    # check total
    for result in resp.data['results']:
        if result['item'] == 'B':
            assert float(result['aggregate']) == 2000
        else:
            assert float(result['aggregate']) == 1000

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
        "business_categories": ['small_business'],
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
        "cfda_title": "Shazam",
    }

    parent_unique_award = {
        "agency_id": "192",
        "referenced_idv_agency_iden": "168",
        "piid": "0",
        "parent_award_id": "1",
    }

    idv_date_obj = {
        "end_date": "2025-06-30",
        "start_date": "2010-09-23",
        "last_modified_date": "2018-08-24"
    }

    latest_transaction_contract_data = {
        "pk": 2,
        "transaction": TransactionNormalized.objects.get(pk=2),
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "naics": "333911",
        "naics_description": "PUMP AND PUMPING EQUIPMENT MANUFACTURING",
        "idv_type_description": "IDC",
        "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        "referenced_idv_agency_iden": None,
        "multiple_or_single_aw_desc": "MULTIPLE AWARD",
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
    mommy.make("awards.TransactionFPDS", **latest_transaction_contract_data)
    award_1_model = {
        "pk": 1,
        "type": "IDV_B_B",
        "category": "idv",
        "piid": 1234,
        "parent_unique_award": parent_unique_award,
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
        "idv_dates": idv_date_obj

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
        "generated_unique_award_id": "CONT_AW_9700_9700_03VD_SPM30012D3486",
        "latest_transaction": TransactionNormalized.objects.get(pk=2),
        "total_subaward_amount": 12345.00,
        "subaward_count": 10,
        "idv_dates": idv_date_obj,
    }
    mommy.make('awards.Award', **award_1_model)
    mommy.make('awards.Award', **award_2_model)


@pytest.mark.django_db
def test_award_last_updated_endpoint(client):
    """Test the awards endpoint."""

    test_date = datetime.datetime.now()
    test_date_reformatted = test_date.strftime('%m/%d/%Y')

    mommy.make('awards.Award', update_date=test_date)
    mommy.make('awards.Award', update_date='')

    resp = client.get('/api/v2/awards/last_updated/')
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data['last_updated'] == test_date_reformatted


@pytest.mark.django_db
def test_award_endpoint_generated_id(client, awards_and_transactions):

    resp = client.get('/api/v2/awards/ASST_AW_3620_-NONE-_1830212.0481163/')
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_asst

    resp = client.get('/api/v2/awards/CONT_AW_9700_9700_03VD_SPM30012D3486/')
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_cont


# @pytest.mark.django_db
# def test_award_endpoint_primary_key(client, awards_and_transactions):
    """Unable to run these in a new test function, so combining with above test function"""
    resp = client.get('/api/v2/awards/1/')
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response_idv

expected_response_idv = {
    "id": 2,
    "type": "A",
    "category": "contract",
    "type_description": "DEFINITIVE CONTRACT",
    "piid": "5678",
    "parent_award_piid": "1234",
    "description": "lorem ipsum",
    "awarding_agency": {
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff"},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff"},
        "office_agency_name": "office_agency",
        "office_agency": {"aac_code": None, "name": "office_agency"},
    },
    "funding_agency": {
        "toptier_agency": {"name": "agency name", "abbreviation": "some other stuff"},
        "subtier_agency": {"name": "agency name", "abbreviation": "some other stuff"},
        "office_agency_name": "office_agency",
        "office_agency": {"aac_code": None, "name": "office_agency"},
    },
    "recipient": {
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
            "zip4": "122045312",
            "foreign_postal_code": None,
            "country_name": "UNITED STATES",
            "location_country_code": "USA",
            "congressional_code": "90",
        },
        "parent_recipient_name": None,
    },
    "total_obligation": "1000.00",
    "base_and_all_options_value": "2000.00",
    "period_of_performance": {
        "period_of_performance_start_date": "2004-02-04",
        "period_of_performance_current_end_date": "2005-02-04",
    },
    "place_of_performance": {
        "address_line1": "123 main st",
        "address_line2": None,
        "address_line3": None,
        "foreign_province": None,
        "city_name": "Charlotte",
        "county_name": "BUNCOMBE",
        "state_code": "NC",
        "zip5": "12204",
        "zip4": "122045312",
        "foreign_postal_code": None,
        "country_name": "UNITED STATES",
        "location_country_code": "USA",
        "congressional_code": "90",
    },
    "latest_transaction_contract_data": {
        "idv_type_description": "IDC",
        "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        "referenced_idv_agency_iden": None,
        "multiple_or_single_aw_desc": "MULTIPLE AWARD",
        "solicitation_identifier": None,
        "solicitation_procedures": "NP",
        "number_of_offers_received": "5",
        "extent_competed": "A",
        "other_than_full_and_o_desc": None,
        "type_set_aside_description": "NO SET ASIDE USED.",
        "commercial_item_acquisitio": "A",
        "commercial_item_test_desc": "NO",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "fed_biz_opps_description": "YES",
        "small_business_competitive": "False",
        "fair_opportunity_limi_desc": None,
        "product_or_service_code": "V126",
        "naics": "336414",
        "dod_claimant_program_code": None,
        "program_system_or_equipmen": None,
        "information_technolog_desc": None,
        "sea_transportation_desc": None,
        "clinger_cohen_act_pla_desc": "NO",
        "ordering_period_end_date": "2025-06-30",
        "construction_wage_rat_desc": "NO",
        "labor_standards_descrip": "NO",
        "materials_supplies_descrip": "NO",
        "cost_or_pricing_data_desc": "NO",
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
        "foreign_funding_desc": "NOT APPLICABLE",
        "interagency_contract_desc": "NOT APPLICABLE",
        "major_program": "LAUNCH SERVICES PROGRAM",
        "price_evaluation_adjustmen": None,
        "program_acronym": "AGVY-LSP",
        "subcontracting_plan": "C",
        "multi_year_contract_desc": "YES",
        "purchase_card_as_paym_desc": None,
        "consolidated_contract_desc": "NO",
        "type_of_contract_pric_desc": "FIRM FIXED PRICE"
    },
    "executive_details": {
        "officers": [
            {
                "name": "Salvatore T Bruno ",
                "amount": "2127685.00"
            },
            {
                "name": "Daniel J Collins ",
                "amount": "1323860.00"
            },
            {
                "name": "Charles G Krisch ",
                "amount": "560304.00"
            },
            {
                "name": "James J DeNapoli ",
                "amount": "475534.00"
            },
            {
                "name": "Robbie K Sabathier ",
                "amount": "463429.00"
            }
        ]
    },
    "base_exercised_options_val": None,
    "idv_dates": {
        "end_date": "2025-06-30",
        "start_date": "2010-09-23",
        "last_modified_date": "2018-08-24"
    }
    "subaward_count": 10,
    "total_subaward_amount": "12345.00",
}
