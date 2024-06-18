import pytest

from model_bakery import baker

from usaspending_api.common.elasticsearch.search_wrappers import SubawardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def subaward_test_data_fixture(db):
    # Non-COVID-19 & non-IIJA disaster code
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="A",
        public_law="PUBLIC LAW FOR CODE A",
        earliest_public_law_enactment_date=None,
    )

    # COVID-19 disaster code
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        public_law="PUBLIC LAW FOR CODE L",
        earliest_public_law_enactment_date="2020-03-06",
        group_name="covid_19",
    )

    # IIJA disaster code
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        public_law="PUBLIC LAW FOR CODE Z",
        earliest_public_law_enactment_date="2021-11-15",
        group_name="infrastructure",
    )

    award_search1 = baker.make(
        "search.AwardSearch",
        award_id=111,
        generated_unique_award_id="UNIQUE_AWARD_ID_1",
        disaster_emergency_fund_codes=["A"],
    )
    award_search2 = baker.make(
        "search.AwardSearch",
        award_id=222,
        generated_unique_award_id="UNIQUE_AWARD_ID_2",
        disaster_emergency_fund_codes=["A", "L"],
    )
    award_search3 = baker.make(
        "search.AwardSearch",
        award_id=333,
        generated_unique_award_id="UNIQUE_AWARD_ID_3",
        disaster_emergency_fund_codes=["Z"],
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        sub_awardee_or_recipient_uniqu="111111111",
        sub_ultimate_parent_unique_ide="111111110",
        sub_awardee_or_recipient_uei="AAAAAAAAAAAA",
        sub_ultimate_parent_uei="AAAAAAAAAAA0",
        award=award_search1,
        sub_action_date="2018-01-01",
        action_date="2018-01-01",
        cfda_numbers="17.277, 17.286",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        sub_awardee_or_recipient_uniqu="222222222",
        sub_ultimate_parent_unique_ide="222222220",
        sub_awardee_or_recipient_uei="BBBBBBBBBBBB",
        sub_ultimate_parent_uei="BBBBBBBBBBB0",
        award=award_search2,
        sub_action_date="2020-04-01",
        action_date="2020-04-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        sub_awardee_or_recipient_uniqu="333333333",
        sub_ultimate_parent_unique_ide="333333330",
        sub_awardee_or_recipient_uei="CCCCCCCCCCCC",
        sub_ultimate_parent_uei="CCCCCCCCCCC0",
        award=award_search3,
        sub_action_date="2022-01-01",
        action_date="2022-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        sub_awardee_or_recipient_uniqu="444444444",
        sub_ultimate_parent_unique_ide="444444440",
        sub_awardee_or_recipient_uei="QQQQQQQQQQQQ",
        sub_ultimate_parent_uei="QQQQQQQQQQQQ0",
        award=award_search3,
        sub_action_date="2020-01-01",
        action_date="2020-01-01",
    )


@pytest.mark.django_db
def test_keyword_filter_duns(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    filters = {"keywords": ["111111111", "222222222", "333"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 2


@pytest.mark.django_db
def test_keyword_filter_uei(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    filters = {"keywords": ["AAAAAAAAAAAA", "BBBBBBBBBBB0", "CCC"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 2


@pytest.mark.django_db
def test_keyword_filter_uei_lowercase(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    filters = {"keywords": ["aaaaaaaaaaaa", "bbbbbbbbbbb0", "ccc"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 2


@pytest.mark.django_db
def test_defc_filter(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    """Test that only the correct subawards are returned when a disaster code
    is provided.
    """

    # Test DEF code `A` which doesn't have an enactment date
    filters = {"def_codes": ["A"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 2

    # Test DEF code `Z`
    # Subaward 4 should not be returned because it's `sub_action_date`
    #   is prior to the enactment date of DEF code `Z`
    filters = {"def_codes": ["Z"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 1

    filters = {"def_codes": ["L"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 1

    filters = {"def_codes": ["L", "Z"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 2


@pytest.mark.django_db
def test_defc_and_date_filters(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    """Test the combination of the `def_codes` and `time_period` filters"""

    # No subawards should be returned, because even though Subaward 4 falls into this `time_period`
    #   it's `sub_action_date` is prior to when DEF code `Z` went into effect and therefore filtered out
    filters = {"def_codes": ["Z"], "time_period": [{"start_date": "2019-01-01", "end_date": "2021-12-01"}]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 0

    filters = {"def_codes": ["A", "L", "Z"], "time_period": [{"start_date": "2018-01-01", "end_date": "2023-01-01"}]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 3


@pytest.mark.django_db
def test_cfda_numbers_filters(client, monkeypatch, elasticsearch_subaward_index, subaward_test_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    filters = {"program_numbers": ["17.277"]}
    filter_query = QueryWithFilters.generate_subawards_elasticsearch_query(filters)
    search = SubawardSearch().filter(filter_query)
    results = search.handle_execute()
    assert len(results) == 1
