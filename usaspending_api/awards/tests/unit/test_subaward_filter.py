import pytest
from model_mommy import mommy
from usaspending_api.awards.v2.filters.sub_award import subaward_filter


@pytest.fixture
def subaward_data(db):
    mommy.make(
        "awards.Subaward",
        id=1,
        recipient_unique_id="111111111",
        parent_recipient_unique_id="111111110",
        recipient_uei="AAAAAAAAAAAA",
        parent_recipient_uei="AAAAAAAAAAA0",
    )

    mommy.make(
        "awards.Subaward",
        id=2,
        recipient_unique_id="222222222",
        parent_recipient_unique_id="222222220",
        recipient_uei="BBBBBBBBBBBB",
        parent_recipient_uei="BBBBBBBBBBB0",
    )

    mommy.make(
        "awards.Subaward",
        id=3,
        recipient_unique_id="333333333",
        parent_recipient_unique_id="333333330",
        recipient_uei="CCCCCCCCCCCC",
        parent_recipient_uei="CCCCCCCCCCC0",
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
