import pytest
from model_bakery import baker

from usaspending_api.awards.v2.filters.sub_award import subaward_filter


@pytest.fixture
def subaward_data(db):
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
    )

    # IIJA disaster code
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        public_law="PUBLIC LAW FOR CODE Z",
        earliest_public_law_enactment_date="2021-11-15",
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
        subaward_description="Description for subaward_1.",
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
        subaward_description="Description for subaward_2.",
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
        subaward_description="Description for subaward_3.",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        sub_awardee_or_recipient_uniqu="444444444",
        sub_ultimate_parent_unique_ide="444444440",
        sub_awardee_or_recipient_uei="CCCCCCCCCCCC",
        sub_ultimate_parent_uei="CCCCCCCCCCC0",
        award=award_search3,
        sub_action_date="2020-01-01",
    )


@pytest.mark.django_db
def test_keyword_filter_duns(subaward_data):
    filters = {"keywords": ["111111111", "222222222", "333"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


@pytest.mark.django_db
def test_keyword_filter_uei(subaward_data):
    filters = {"keywords": ["AAAAAAAAAAAA", "BBBBBBBBBBB0", "CCC"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


@pytest.mark.django_db
def test_keyword_filter_uei_lowercase(subaward_data):
    filters = {"keywords": ["aaaaaaaaaaaa", "bbbbbbbbbbb0", "ccc"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


@pytest.mark.django_db
def test_defc_filter(subaward_data):
    """Test that only the correct subawards are returned when a disaster code
    is provided.
    """

    # Test DEF code `A` which doesn't have an enactment date
    filters = {"def_codes": ["A"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2

    # Test DEF code `Z`
    # Subaward 4 should not be returned because it's `sub_action_date`
    #   is prior to the enactment date of DEF code `Z`
    filters = {"def_codes": ["Z"]}
    results = subaward_filter(filters).all()
    assert len(results) == 1

    filters = {"def_codes": ["L"]}
    results = subaward_filter(filters).all()
    assert len(results) == 1

    filters = {"def_codes": ["L", "Z"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


@pytest.mark.django_db
def test_defc_and_date_filters(subaward_data):
    """Test the combination of the `def_codes` and `time_period` filters"""

    # No subawards should be returned, because even though Subaward 4 falls into this `time_period`
    #   it's `sub_action_date` is prior to when DEF code `Z` went into effect and therefore filtered out
    filters = {"def_codes": ["Z"], "time_period": [{"start_date": "2019-01-01", "end_date": "2021-12-01"}]}
    results = subaward_filter(filters).all()
    assert len(results) == 0

    filters = {"def_codes": ["A", "L", "Z"], "time_period": [{"start_date": "2018-01-01", "end_date": "2023-01-01"}]}
    results = subaward_filter(filters).all()
    assert len(results) == 3


@pytest.mark.django_db
def test_description_filter(subaward_data):

    filters = {"description": "subaward_1"}
    results = subaward_filter(filters).all()
    assert len(results) == 1

    filters = {"description": "subaward"}
    results = subaward_filter(filters).all()
    assert len(results) == 3
