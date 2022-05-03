import json
import pytest

from rest_framework import status
from model_bakery import baker

from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.v2.views.spending_over_time import GROUPING_LOOKUP


@pytest.fixture
def spending_over_time_test_data():
    """
    Generate minimum test data for 'spending_over_time' integration tests.
    Use some calculations inside of a loop to get a larger data sample.
    """
    for i in range(30):
        # Define some values that are calculated and used multiple times
        transaction_id = i
        award_id = i + 1000
        awarding_agency_id = i + 2000
        toptier_awarding_agency_id = i + 3000
        subtier_awarding_agency_id = i + 4000
        funding_agency_id = i + 5000
        toptier_funding_agency_id = i + 6000
        subtier_funding_agency_id = i + 7000
        federal_action_obligation = i + 8000
        total_obligation = i + 9000
        federal_account_id = i + 10000
        treasury_account_id = i + 11000

        action_date = f"20{i % 10 + 10}-{i % 9 + 1}-{i % 28 + 1}"
        contract_award_type = ["A", "B", "C", "D"][i % 4]
        grant_award_type = ["02", "03", "04", "05"][i % 4]
        is_fpds = i % 2 == 0

        # Transaction Normalized
        baker.make(
            "awards.TransactionNormalized",
            id=transaction_id,
            action_date=action_date,
            award_id=award_id,
            awarding_agency_id=awarding_agency_id,
            business_categories=[f"business_category_1_{transaction_id}", f"business_category_2_{transaction_id}"],
            description=f"This is a test description {transaction_id}" if transaction_id % 2 == 0 else None,
            federal_action_obligation=federal_action_obligation,
            funding_agency_id=funding_agency_id,
            is_fpds=is_fpds,
            type=contract_award_type if is_fpds else grant_award_type,
        )

        # Award
        baker.make(
            "awards.Award",
            id=award_id,
            fain=f"fain_{transaction_id}" if not is_fpds else None,
            is_fpds=is_fpds,
            latest_transaction_id=transaction_id,
            piid=f"piid_{transaction_id}" if is_fpds else None,
            total_obligation=total_obligation,
            type=contract_award_type if is_fpds else grant_award_type,
        )

        # Federal, Treasury, and Financial Accounts
        baker.make(
            "accounts.FederalAccount",
            id=federal_account_id,
            parent_toptier_agency_id=toptier_awarding_agency_id,
            account_title=f"federal_account_title_{transaction_id}",
            federal_account_code=f"federal_account_code_{transaction_id}",
        )
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            agency_id=f"taa_aid_{transaction_id}",
            allocation_transfer_agency_id=f"taa_ata_{transaction_id}",
            availability_type_code=f"taa_a_{transaction_id}",
            beginning_period_of_availability=f"taa_bpoa_{transaction_id}",
            ending_period_of_availability=f"taa_epoa_{transaction_id}",
            federal_account_id=federal_account_id,
            main_account_code=f"taa_main_{transaction_id}",
            sub_account_code=f"taa_sub_{transaction_id}",
            treasury_account_identifier=treasury_account_id,
        )
        baker.make("awards.FinancialAccountsByAwards", award_id=award_id, treasury_account_id=treasury_account_id)

        # Awarding Agency
        baker.make(
            "references.Agency",
            id=awarding_agency_id,
            subtier_agency_id=subtier_awarding_agency_id,
            toptier_agency_id=toptier_awarding_agency_id,
        )
        baker.make(
            "references.ToptierAgency",
            abbreviation=f"toptier_awarding_agency_abbreviation_{transaction_id}",
            name=f"toptier_awarding_agency_agency_name_{transaction_id}",
            toptier_agency_id=toptier_awarding_agency_id,
            toptier_code=f"toptier_awarding_agency_code_{transaction_id}",
        )
        baker.make(
            "references.SubtierAgency",
            abbreviation=f"subtier_awarding_agency_abbreviation_{transaction_id}",
            name=f"subtier_awarding_agency_agency_name_{transaction_id}",
            subtier_agency_id=subtier_awarding_agency_id,
            subtier_code=f"subtier_awarding_agency_code_{transaction_id}",
        )

        # Funding Agency
        baker.make(
            "references.Agency",
            id=funding_agency_id,
            subtier_agency_id=subtier_funding_agency_id,
            toptier_agency_id=toptier_funding_agency_id,
        )
        baker.make(
            "references.ToptierAgency",
            abbreviation=f"toptier_funding_agency_abbreviation_{transaction_id}",
            name=f"toptier_funding_agency_agency_name_{transaction_id}",
            toptier_agency_id=toptier_funding_agency_id,
            toptier_code=f"toptier_funding_agency_code_{transaction_id}",
        )
        baker.make(
            "references.SubtierAgency",
            abbreviation=f"subtier_funding_agency_abbreviation_{transaction_id}",
            name=f"subtier_funding_agency_agency_name_{transaction_id}",
            subtier_agency_id=subtier_funding_agency_id,
            subtier_code=f"subtier_funding_agency_code_{transaction_id}",
        )

        # Ref Country Code
        baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")

        # FPDS / FABS
        if is_fpds:
            baker.make(
                "awards.TransactionFPDS",
                awardee_or_recipient_legal=f"recipient_name_{transaction_id}",
                awardee_or_recipient_uniqu=f"{transaction_id:09d}",
                extent_competed=f"extent_competed_{transaction_id}",
                legal_entity_country_code="USA",
                legal_entity_country_name="USA",
                legal_entity_state_code=f"LE_STATE_CODE_{transaction_id}",
                legal_entity_county_code=f"{transaction_id:03d}",
                legal_entity_county_name=f"LE_COUNTY_NAME_{transaction_id}",
                legal_entity_congressional=f"{transaction_id:02d}",
                legal_entity_zip5=f"LE_ZIP5_{transaction_id}",
                legal_entity_city_name=f"LE_CITY_NAME_{transaction_id}",
                naics=f"{transaction_id}{transaction_id}",
                piid=f"piid_{transaction_id}",
                place_of_perform_country_c="USA",
                place_of_perf_country_desc="UNITED STATES",
                place_of_performance_state=f"POP_STATE_CODE_{transaction_id}",
                place_of_perform_county_co=f"{transaction_id:03d}",
                place_of_perform_county_na=f"POP_COUNTY_NAME_{transaction_id}",
                place_of_performance_zip5=f"POP_ZIP5_{transaction_id}",
                place_of_performance_congr=f"{transaction_id:02d}",
                place_of_perform_city_name=f"POP_CITY_NAME_{transaction_id}",
                product_or_service_code=str(transaction_id).zfill(4),
                transaction_id=transaction_id,
                type_of_contract_pricing=f"type_of_contract_pricing_{transaction_id}",
                type_set_aside=f"type_set_aside_{transaction_id}",
            )
            baker.make(
                "references.NAICS",
                code=f"naics_code_{transaction_id}",
                description=f"naics_description_{transaction_id}",
            )
            baker.make("references.PSC", code=f"ps{transaction_id}", description=f"psc_description_{transaction_id}")
        else:
            baker.make(
                "awards.TransactionFABS",
                awardee_or_recipient_legal=f"recipient_name_{transaction_id}",
                awardee_or_recipient_uniqu=f"{transaction_id:09d}",
                cfda_number=f"cfda_number_{transaction_id}",
                fain=f"fain_{transaction_id}",
                legal_entity_country_code="USA",
                legal_entity_country_name="USA",
                legal_entity_state_code=f"LE_STATE_CODE_{transaction_id}",
                legal_entity_county_code=f"{transaction_id:03d}",
                legal_entity_county_name=f"LE_COUNTY_NAME_{transaction_id}",
                legal_entity_congressional=f"{transaction_id:02d}",
                legal_entity_zip5=f"LE_ZIP5_{transaction_id}",
                legal_entity_city_name=f"LE_CITY_NAME_{transaction_id}",
                place_of_perform_country_c="USA",
                place_of_perform_country_n="UNITED STATES",
                place_of_perfor_state_code=f"POP_STATE_CODE_{transaction_id}",
                place_of_perform_county_co=f"{transaction_id:03d}",
                place_of_perform_county_na=f"POP_COUNTY_NAME_{transaction_id}",
                place_of_performance_zip5=f"POP_ZIP5_{transaction_id}",
                place_of_performance_congr=f"{transaction_id:02d}",
                place_of_performance_city=f"POP_CITY_NAME{transaction_id}",
                transaction_id=transaction_id,
            )


@pytest.mark.django_db
def test_spending_over_time_success(client, monkeypatch, elasticsearch_transaction_index):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # test for needed filters
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "fiscal_year", "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_over_time_failure(client, monkeypatch, elasticsearch_transaction_index):
    """Verify error on bad autocomplete request for budget function."""

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({"group": "fiscal_year"})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_over_time_subawards_success(client):

    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": non_legacy_filters(), "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_over_time_subawards_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": non_legacy_filters(), "subawards": "string"}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    for group in GROUPING_LOOKUP.keys():
        resp = client.post(
            "/api/v2/search/spending_over_time",
            content_type="application/json",
            data=json.dumps({"group": group, "filters": non_legacy_filters()}),
        )
        assert resp.status_code == status.HTTP_200_OK, f"Failed to return 200 Response for group: {group}"


@pytest.mark.django_db
def test_correct_response_for_each_filter(
    client, monkeypatch, spending_over_time_test_data, elasticsearch_transaction_index
):
    """
    Verify the content of the response when using different filters. This function creates the ES Index
    and then calls each of the tests instead of recreating the ES Index multiple times with the same data.
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_cases = [
        _test_correct_response_for_keywords,
        _test_correct_response_for_time_period,
        _test_correct_response_for_award_type_codes,
        _test_correct_response_for_agencies,
        _test_correct_response_for_tas_codes,
        _test_correct_response_for_pop_location,
        _test_correct_response_for_recipient_location,
        _test_correct_response_for_recipient_search_text,
        _test_correct_response_for_recipient_type_names,
        _test_correct_response_for_award_amounts,
        _test_correct_response_for_cfda_program,
        _test_correct_response_for_naics_codes,
        _test_correct_response_for_psc_codes,
        _test_correct_response_for_contract_pricing_type_codes,
        _test_correct_response_for_set_aside_type_codes,
        _test_correct_response_for_set_extent_competed_type_codes,
        _test_correct_response_for_recipient_id,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_keywords(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "keywords": ["test", "recipient_name_1"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 24030.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 16012.0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 24036.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 8013.0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 24042.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 8015.0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 24048.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 8017, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 24054.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 8019, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Keyword filter does not match expected result"


def _test_correct_response_for_time_period(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "time_period": [
                        {"start_date": "2012-01-01", "end_date": "2013-09-30"},
                        {"start_date": "2015-01-01", "end_date": "2016-09-30"},
                        {"start_date": "2017-08-01", "end_date": "2018-05-30"},
                    ]
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 24036.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 24039.0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 24045.0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 24048.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 16024.0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 16046.0, "time_period": {"fiscal_year": "2018"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Time Period filter does not match expected result"


def _test_correct_response_for_award_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 24030.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 24036.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 24042.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 24048.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 24054.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Award Type Codes filter does not match expected result"


def _test_correct_response_for_agencies(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "agencies": [
                        {"type": "awarding", "tier": "toptier", "name": "toptier_awarding_agency_agency_name_4"},
                        {"type": "awarding", "tier": "subtier", "name": "subtier_awarding_agency_agency_name_4"},
                        {"type": "funding", "tier": "toptier", "name": "toptier_funding_agency_agency_name_4"},
                        {"type": "funding", "tier": "subtier", "name": "subtier_funding_agency_agency_name_4"},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 8004.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Agency filter does not match expected result"


def _test_correct_response_for_tas_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "tas_codes": [
                        {
                            "ata": "taa_ata_3",
                            "aid": "taa_aid_3",
                            "bpoa": "taa_bpoa_3",
                            "epoa": "taa_epoa_3",
                            "main": "taa_main_3",
                            "sub": "taa_sub_3",
                        },
                        {
                            "ata": "taa_ata_5",
                            "aid": "taa_aid_5",
                            "bpoa": "taa_bpoa_5",
                            "epoa": "taa_epoa_5",
                            "main": "taa_main_5",
                            "sub": "taa_sub_5",
                        },
                        {
                            "ata": "taa_ata_15",
                            "aid": "taa_aid_15",
                            "bpoa": "taa_bpoa_15",
                            "epoa": "taa_epoa_15",
                            "main": "taa_main_15",
                            "sub": "taa_sub_15",
                        },
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 8003.0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 16020.0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "TAS Codes filter does not match expected result"


def _test_correct_response_for_pop_location(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "place_of_performance_locations": [
                        {"country": "USA", "state": "pop_state_code_2", "city": "pop_city_name_2"},
                        {"country": "USA", "state": "pop_state_code_12", "county": "012"},
                        {"country": "USA", "state": "pop_state_code_18", "district": "18"},
                        {"country": "USA", "zip": "pop_zip5_19"},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 16014.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 8018.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 8019.0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Place of Performance filter does not match expected result"


def _test_correct_response_for_recipient_location(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "recipient_locations": [
                        {"country": "USA", "state": "le_state_code_4", "city": "le_city_name_4"},
                        {"country": "USA", "state": "le_state_code_7", "county": "007"},
                        {"country": "USA", "state": "le_state_code_17", "district": "17"},
                        {"country": "USA", "zip": "le_zip5_20"},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 8020.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 8004.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 16024.0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Recipient Location filter does not match expected result"


def _test_correct_response_for_recipient_search_text(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "recipient_search_text": ["recipient_name_10", "recipient_name_14", "000000020"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 16030.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 8014.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Recipient Search Text filter does not match expected result"


def _test_correct_response_for_recipient_type_names(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "recipient_type_names": ["business_category_1_3", "business_category_2_8"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 8003.0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 8008.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Recipient Type Names filter does not match expected result"


def _test_correct_response_for_award_amounts(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "award_amounts": [
                        {"upper_bound": 9001},
                        {"lower_bound": 9013, "upper_bound": 9017},
                        {"lower_bound": 9027},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 8000.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 8001.0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 8013.0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 8014.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 8015.0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 8016.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 16044.0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 8028.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 8029.0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Award Amounts filter does not match expected result"


def _test_correct_response_for_cfda_program(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "program_numbers": ["cfda_number_11", "cfda_number_21", "cfda_number_25"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 16032.0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 8025.0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "CFDA Program filter does not match expected result"


def _test_correct_response_for_naics_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "naics_codes": {"require": ["88", "1616", "2626"]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 16042.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 8008.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "NAICS Code filter does not match expected result"


def _test_correct_response_for_psc_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "psc_codes": ["0002", "0012", "0024"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 16014.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 8024.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_correct_response_for_contract_pricing_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "contract_pricing_type_codes": [
                        "type_of_contract_pricing_0",
                        "type_of_contract_pricing_10",
                        "type_of_contract_pricing_22",
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 16010.0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 8022.0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert (
        resp.json().get("results") == expected_result
    ), "Contract Pricing Type Codes filter does not match expected result"


def _test_correct_response_for_set_aside_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "set_aside_type_codes": ["type_set_aside_16", "type_set_aside_26", "type_set_aside_28"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 16042.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 8028.0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Set Aside Type Codes filter does not match expected result"


def _test_correct_response_for_set_extent_competed_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "extent_competed_type_codes": ["extent_competed_4", "extent_competed_24", "extent_competed_26"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 16028.0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 8026.0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert (
        resp.json().get("results") == expected_result
    ), "Extent Competed Type Codes filter does not match expected result"


def _test_correct_response_for_recipient_id(client):
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps(
            {
                "group": "fiscal_year",
                "filters": {
                    "recipient_id": "c687823d-10af-701b-1bad-650c6e680190-R",
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_result = [
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2008"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2009"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010"}},
        {"aggregated_amount": 8021.0, "time_period": {"fiscal_year": "2011"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2012"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2013"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2014"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2015"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2016"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2017"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2019"}},
        {"aggregated_amount": 0, "time_period": {"fiscal_year": "2020"}},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json().get("results") == expected_result, "Recipient ID filter does not match expected result"


@pytest.mark.django_db
def test_failure_with_invalid_filters(client, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Fails with no filters
    resp = client.post(
        "/api/v2/search/spending_over_time", content_type="application/json", data=json.dumps({"group": "fiscal_year"})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json().get("detail") == "Missing request parameters: filters", "Expected to fail with missing filters"

    # Fails with empty filters
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "fiscal_year", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json().get("detail") == "Missing request parameters: filters", "Expected to fail with empty filters"


@pytest.mark.django_db
def test_failure_with_invalid_group(client, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Fails with wrong group
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "not a valid group", "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.json().get("detail")
        == "Field 'group' is outside valid values ['quarter', 'q', 'fiscal_year', 'fy', 'month', 'm']"
    ), "Expected to fail with invalid group"

    # Fails with no group
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json().get("detail") == "Missing value: 'group' is a required field", "Expected to fail with no group"


@pytest.mark.django_db
def test_defc_date_filter(client, monkeypatch, elasticsearch_transaction_index):
    defc1 = baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        title="TITLE FOR CODE L",
        group_name="covid_19",
    )
    baker.make("accounts.FederalAccount", id=99)
    baker.make("accounts.TreasuryAppropriationAccount", federal_account_id=99, treasury_account_identifier=99)
    baker.make(
        "awards.FinancialAccountsByAwards", pk=1, award_id=99, disaster_emergency_fund=defc1, treasury_account_id=99
    )
    baker.make("awards.Award", id=99, total_obligation=20, piid="0001")
    baker.make(
        "awards.TransactionNormalized",
        id=99,
        action_date="2020-04-02",
        federal_action_obligation=10,
        award_id=99,
        is_fpds=True,
        type="A",
    )
    baker.make("awards.TransactionFPDS", transaction_id=99, piid="0001")
    baker.make(
        "awards.TransactionNormalized",
        id=100,
        action_date="2020-01-01",
        federal_action_obligation=22,
        award_id=99,
        is_fpds=True,
        type="A",
    )
    baker.make("awards.TransactionFPDS", transaction_id=100)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "fiscal_year", "filters": {"def_codes": ["L"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert {"aggregated_amount": 10, "time_period": {"fiscal_year": "2020"}} in resp.json().get("results")
