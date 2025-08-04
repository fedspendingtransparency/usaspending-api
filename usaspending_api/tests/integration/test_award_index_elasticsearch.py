import pytest

from model_bakery import baker


@pytest.fixture
def award_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        type="A",
        recipient_location_zip5="abcde",
        piid="IND12PB00323",
        recipient_location_county_code="059",
        recipient_location_state_code="VA",
        recipient_location_congressional_code="11",
        recipient_location_country_code="USA",
        pop_state_code="VA",
        pop_congressional_code="11",
        place_of_performance_code="USA",
        naics_code="331122",
        product_or_service_code="1510",
        type_set_aside="8AN",
        type_of_contract_pricing="2",
        extent_competed="F",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=False,
        transaction_id=2,
        award_id=2,
        action_date="2016-10-01",
        type="02",
        fain="P063P100612",
        cfda_number="84.063",
    )
    baker.make(
        "references.ToptierAgency", toptier_agency_id=1, name="Department of Transportation", _fill_optional=True
    )
    baker.make(
        "references.SubtierAgency", subtier_agency_id=1, name="Department of Transportation", _fill_optional=True
    )
    baker.make("references.Agency", id=1, toptier_agency_id=1, subtier_agency_id=1, _fill_optional=True)
    baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        display_award_id="IND12PB00323",
        description="pop tarts and assorted cereals",
        total_obligation=500000.00,
        date_signed="2010-10-1",
        awarding_agency_id=1,
        funding_agency_id=1,
        action_date="2010-10-1",
        recipient_location_county_code="059",
        recipient_location_state_code="VA",
        recipient_location_congressional_code="11",
        recipient_location_country_code="USA",
        pop_state_code="VA",
        pop_congressional_code="11",
        pop_country_code="USA",
        naics_code="331122",
        product_or_service_code="1510",
        type_set_aside="8AN",
        type_of_contract_pricing="2",
        extent_competed="F",
        tas_paths="{aid=097main=4930ata=sub=000bpoa=epoa=a=X}",
        funding_toptier_agency_name="Department of Transportation",
        disaster_emergency_fund_codes="{L}",
        recipient_name="GENERAL MOTORS CORPORATION",
        parent_recipient_name="PARENT GENERAL MOTORS CORPORATION",
        recipient_uei="GVAKTC49BNJ8",
        parent_uei="GVAKTC49BNJ8",
        recipient_unique_id="005356613",
        parent_recipient_unique_id="005356613",
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        latest_transaction_id=2,
        is_fpds=False,
        type="02",
        fain="P063P100612",
        display_award_id="P063P100612",
        cfda_number="84.063",
        total_obligation=1000000.00,
        date_signed="2016-10-1",
        action_date="2016-10-1",
    )
    baker.make(
        "accounts.FederalAccount", id=1, parent_toptier_agency_id=1, agency_identifier="1", main_account_code="0001"
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=1,
        agency_id="097",
        main_account_code="4930",
        federal_account_id=1,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        reporting_period_start="2020-04-21",
        reporting_period_end="2020-04-30",
        quarter_format_flag=True,
        submission_window_id=2020121,
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020121,
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        is_quarter=True,
        period_start_date="2020-04-21",
        submission_reveal_date="2020-04-30",
    )
    code = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award_id=1,
        treasury_account_id=1,
        submission_id=1,
        disaster_emergency_fund=code,
        gross_outlay_amount_by_award_cpe=100,
        transaction_obligated_amount=100,
    )
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


def create_query(should) -> dict:
    query = {
        "query": {"bool": {"filter": {"bool": {"should": should, "minimum_should_match": 1}}}},
        "_source": ["award_id"],
    }
    return query


@pytest.mark.django_db
def test_date_range(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {
        "bool": {
            "should": [
                {"range": {"action_date": {"gte": "2010-10-01"}}},
                {"range": {"date_signed": {"lte": "2011-09-30"}}},
            ],
            "minimum_should_match": 2,
        }
    }
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {
        "bool": {
            "should": [
                {"range": {"action_date": {"gte": "2011-10-01"}}},
                {"range": {"date_signed": {"lte": "2012-09-30"}}},
            ],
            "minimum_should_match": 2,
        }
    }
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_tas(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()

    tas_code_regexes1 = {
        "aid": "097",
        "ata": ".*",
        "main": "4930",
        "sub": ".*",
        "bpoa": ".*",
        "epoa": ".*",
        "a": ".*",
    }
    value_regex1 = f".*aid={tas_code_regexes1['aid']}main={tas_code_regexes1['main']}.*"
    should = {"regexp": {"tas_paths": {"value": value_regex1}}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    tas_code_regexes2 = {
        "aid": "028",
        "ata": ".*",
        "main": "8006",
        "sub": ".*",
        "bpoa": ".*",
        "epoa": ".*",
        "a": ".*",
    }
    value_regex2 = f".*aid={tas_code_regexes2['aid']}main={tas_code_regexes2['main']}.*"
    should = {"regexp": {"tas_paths": {"value": value_regex2}}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_agency(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"funding_toptier_agency_name.keyword": "Department of Transportation"}}
    client = elasticsearch_award_index.client
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    elasticsearch_award_index.update_index()
    should = {"match": {"funding_toptier_agency_name.keyword": "Department of Labor"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_award_type(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"type": "A"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"type": "D"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_award_id(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"display_award_id": "IND12PB00323"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"display_award_id": "whatever"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_cfda(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"cfda_number.keyword": "84.063"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"cfda_number.keyword": "whatever"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_recipient_location(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = [
        {"match": {"recipient_location_country_code": "USA"}},
        {"match": {"recipient_location_state_code": "VA"}},
        {"match": {"recipient_location_congressional_code": "11"}},
    ]
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 3
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1

    should = [
        {"match": {"recipient_location_country_code": "USA"}},
        {"match": {"recipient_location_state_code": "VA"}},
        {"match": {"recipient_location_congressional_code": "10"}},
    ]
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 3
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_parent_fields(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"parent_recipient_name": "PARENT GENERAL MOTORS CORPORATION"}}
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 1
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1

    should = {"match": {"parent_uei": "GVAKTC49BNJ8"}}
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 1
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1

    should = {"match": {"parent_recipient_unique_id": "005356613"}}
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 1
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1


@pytest.mark.django_db
def test_pop_location(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = [
        {"match": {"pop_country_code": "USA"}},
        {"match": {"pop_state_code": "VA"}},
        {"match": {"pop_congressional_code": "11"}},
    ]
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 3
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = [
        {"match": {"pop_country_code": "USA"}},
        {"match": {"pop_state_code": "VA"}},
        {"match": {"pop_congressional_code": "10"}},
    ]
    query = create_query(should)
    query["query"]["bool"]["filter"]["bool"]["minimum_should_match"] = 3
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_naics(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"naics_code.keyword": "331122"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"naics_code.keyword": "331120"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_psc(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"product_or_service_code.keyword": "1510"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"product_or_service_code.keyword": "1511s"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_type_set_aside(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"type_set_aside.keyword": "8AN"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"type_set_aside.keyword": "8A"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_contract_pricing(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"type_of_contract_pricing.keyword": "2"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"type_of_contract_pricing.keyword": "1"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_extent_competed(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    should = {"match": {"extent_competed": "F"}}
    query = create_query(should)
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    should = {"match": {"extent_competed": "J"}}
    query = create_query(should)
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_award_keyword(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    query = {
        "query": {"bool": {"filter": {"dis_max": {"queries": [{"query_string": {"query": "pop tarts"}}]}}}},
        "_source": ["award_id"],
    }
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    query = {
        "query": {
            "bool": {
                "filter": {
                    "dis_max": {
                        "queries": [
                            {"query_string": {"query": "jonathan simms", "fields": ["description", "recipient_name"]}}
                        ]
                    }
                }
            }
        },
        "_source": ["award_id"],
    }
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 0


@pytest.mark.django_db
def test_covid_data(award_data_fixture, elasticsearch_award_index):
    elasticsearch_award_index.update_index()
    query = {
        "query": {
            "bool": {
                "filter": {
                    "bool": {"should": {"match": {"disaster_emergency_fund_codes": "L"}}, "minimum_should_match": 1}
                }
            }
        }
    }
    client = elasticsearch_award_index.client
    response = client.search(index=elasticsearch_award_index.index_name, body=query)
    assert response["hits"]["total"]["value"] == 1
    assert response["hits"]["hits"][0]["_source"]["disaster_emergency_fund_codes"] == ["L"]
