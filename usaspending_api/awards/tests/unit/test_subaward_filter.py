import pytest
from model_bakery import baker
from usaspending_api.awards.v2.filters.sub_award import subaward_filter


@pytest.fixture
def subaward_data(db):
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        sub_awardee_or_recipient_uniqu="111111111",
        sub_ultimate_parent_unique_ide="111111110",
        sub_awardee_or_recipient_uei="AAAAAAAAAAAA",
        sub_ultimate_parent_uei="AAAAAAAAAAA0",
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        sub_awardee_or_recipient_uniqu="222222222",
        sub_ultimate_parent_unique_ide="222222220",
        sub_awardee_or_recipient_uei="BBBBBBBBBBBB",
        sub_ultimate_parent_uei="BBBBBBBBBBB0",
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        sub_awardee_or_recipient_uniqu="333333333",
        sub_ultimate_parent_unique_ide="333333330",
        sub_awardee_or_recipient_uei="CCCCCCCCCCCC",
        sub_ultimate_parent_uei="CCCCCCCCCCC0",
    )


def test_keyword_filter_duns(subaward_data):
    filters = {"keywords": ["111111111", "222222222", "333"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


def test_keyword_filter_uei(subaward_data):
    filters = {"keywords": ["AAAAAAAAAAAA", "BBBBBBBBBBB0", "CCC"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2


def test_keyword_filter_uei_lowercase(subaward_data):
    filters = {"keywords": ["aaaaaaaaaaaa", "bbbbbbbbbbb0", "ccc"]}
    results = subaward_filter(filters).all()
    assert len(results) == 2
