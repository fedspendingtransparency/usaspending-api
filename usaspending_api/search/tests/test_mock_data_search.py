import pytest
from model_mommy import mommy

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.references.models import \
    Location, Agency, ToptierAgency, SubtierAgency


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
