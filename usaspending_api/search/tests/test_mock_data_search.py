import pytest
from model_mommy import mommy

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.references.models import \
    Location, Agency, ToptierAgency, SubtierAgency
from usaspending_api.awards.models_matviews import UniversalTransactionView, UniversalAwardView


@pytest.fixture
def budget_function_data():

    country1 = mommy.make(
        'references.RefCountryCode',
        country_code='USA'
    )

    loc1 = mommy.make(
        Location,
        location_id=1,
        location_country_code=country1)

    ttagency1 = mommy.make(
        ToptierAgency,
        toptier_agency_id=1)
    stagency1 = mommy.make(
        SubtierAgency,
        subtier_agency_id=1,)

    agency1 = mommy.make(
        Agency,
        id=1,
        toptier_agency=ttagency1,
        subtier_agency=stagency1)

    award1 = mommy.make(
        Award,
        id=1,
        description="test",
        type="011",
        category="business",
        period_of_performance_start_date="1111-11-11",
        place_of_performance=loc1,
        awarding_agency=agency1,
        funding_agency=agency1,
        total_obligation=1000000.10)

    trans1 = mommy.make(
        TransactionNormalized,
        action_date="2222-2-22",
        id=1,
        award=award1,
        federal_action_obligation=50)

    matview_trans1 = mommy.make(
        UniversalTransactionView,
        action_date="2222-2-22",
        federal_action_obligation=50,
        transaction_id=1,
        fiscal_year=2222,
        type='A',
        action_type='A',
        award_id=2,
        award_category='',
        total_obligation=2876,
        total_obl_bin='<1M',
        fain='dfds',
        uri='dfds',
        piid='dfds',
        pop_location_id='dfds',
        pop_country_name='dfds',
        pop_country_code='dfds',
        pop_zip5='dfds',
        pop_county_code='dfds',
        pop_county_name='dfds',
        pop_state_code='dfds',
        pop_congressional_code='dfds',
        issued_date="2222-2-22",
        face_value_loan_guarantee='dfds',
        original_loan_subsidy_cost='dfds',
        transaction_description='dfds',
        awarding_agency_id='dfds',
        awarding_agency_code='dfds',
        awarding_agency_name='dfds',
        funding_agency_id='dfds',
        funding_agency_code='dfds',
        funding_agency_name='dfds',
        naics_code='dfds',
        naics_description='dfds',
        psc_code='dfds',
        psc_description='dfds',
        type_of_contract_pricing='dfds',
        type_set_aside='dfds',
        extent_competed='dfds',
        cfda_number='dfds',
        cfda_title='dfds',
        cfda_popular_name='dfds',
        recipient_id='dfds',
        recipient_name='dfds',
        recipient_unique_id='dfds',
        parent_recipient_unique_id='dfds',
        business_types_description='dfds',
        recipient_location_id='dfds',
        recipient_location_country_code='dfds',
        recipient_location_country_name='dfds',
        recipient_location_zip5='dfds',
        recipient_location_state_code='dfds',
        recipient_location_state_name='dfds',
        recipient_location_county_code='dfds',
        recipient_location_county_name='dfds',
        recipient_location_congressional_code='dfds',
        awarding_toptier_agency_name='dfds',
        funding_toptier_agency_name='dfds',
        awarding_subtier_agency_name='dfds',
        funding_subtier_agency_name='dfds',
        awarding_toptier_agency_abbreviation='dfds',
        funding_toptier_agency_abbreviation='dfds',
        awarding_subtier_agency_abbreviation='dfds',
        funding_subtier_agency_abbreviation='dfds')

    matview_award1 = mommy.make(
        UniversalAwardView,
        action_date="2222-2-22",
        id=1,
        federal_action_obligation=50,
        award_id=1,
        category='dfds',
        latest_transaction_id='dfds',
        award_type='dfds',
        type_description='dfds',
        description='dfds',
        piid='dfds',
        fain='dfds',
        uri='dfds',
        total_obligation='dfds',
        period_of_performance_start_date='dfds',
        period_of_performance_current_end_date='dfds',
        date_signed='dfds',
        base_and_all_options_value='dfds',
        recipient_id='dfds',
        recipient_name='dfds',
        recipient_unique_id='dfds',
        parent_recipient_unique_id='dfds',
        issued_date='dfds',
        issued_date_fiscal_year=2222,
        face_value_loan_guarantee='dfds',
        original_loan_subsidy_cost='dfds',
        awarding_toptier_agency_name='dfds',
        funding_toptier_agency_name='dfds',
        awarding_subtier_agency_name='dfds',
        funding_subtier_agency_name='dfds',
        awarding_agency_office_name='dfds',
        funding_agency_office_name='dfds',
        recipient_location_address_line1='dfds',
        recipient_location_address_line2='dfds',
        recipient_location_address_line3='dfds',
        recipient_location_country_name='dfds',
        recipient_location_state_code='dfds',
        ecipient_location_foreign_province='dfds',
        recipient_location_county_name='dfds',
        recipient_location_city_name='dfds',
        recipient_location_zip5='dfds',
        recipient_location_congressional_code='dfds',
        pop_city_name='dfds',
        pop_zip5='dfds',
        pop_country_name='dfds',
        pop_state_name='dfds',
        pop_foreign_province='dfds',
        pop_congressional_code='dfds',
        pop_county_name='dfds',
        cfda_number='dfds',
        pulled_from='dfds')


def all_filters():
    return {
        "keyword": "test",
        "time_period": [
            {
                "start_date": "2016-10-01",
                "end_date": "2017-09-30"
            }
        ],
        "agencies": [
            {
                "type": "funding",
                "tier": "toptier",
                "name": "Office of Pizza"
            },
            {
                "type": "awarding",
                "tier": "subtier",
                "name": "Personal Pizza"
            }
        ],
        "legal_entities": [1, 2, 3],
        'recipient_scope': "domestic",
        "recipient_locations": [{"country": "XYZ"},
                                {"country": "USA"},
                                {"country": "ABC"}
                                ],
        "recipient_type_names": [
            "Small Business",
            "Alaskan Native Owned Business"],
        "place_of_performance_scope": "domestic",
        "place_of_performance_locations": [{"country": "USA"},
                                           {"country": "PQR"}],
        "award_type_codes": ["A", "B", "03", '011', '020'],
        "award_ids": [1, 2, 3],
        "award_amounts": [
            {
                "lower_bound": 1000000.00,
                "upper_bound": 25000000.00
            },
            {
                "upper_bound": 1000000.00
            },
            {
                "lower_bound": 500000000.00
            }
        ],
        "program_numbers": ["10.553"],
        "naics_codes": ["336411"],
        "psc_codes": ["1510"],
        "contract_pricing_type_codes": ["SAMPLECODE_CPTC"],
        "set_aside_type_codes": ["SAMPLECODE123"],
        "extent_competed_type_codes": ["SAMPLECODE_ECTC"]
    }
