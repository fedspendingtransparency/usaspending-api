from datetime import datetime

import pytest
from model_bakery import baker

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import (
    _check_awards_for_pre_fy2008,
)


@pytest.fixture
def award_search_data_fixture(db):
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2024-01-15",
        update_date="2024-01-10 17:02:50.223 -0500",
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="TEST_AWARD_2",
        action_date="2020-01-01",
        update_date="2022-01-10 14:02:50.223 -0500",
    )


@pytest.mark.django_db(transaction=True)
def test_find_modified_awards_before_fy2008_non_spark(award_search_data_fixture):
    """Test that we can find any awards that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01), but have
    have been recently updated to have an `action_date` before FY2008 now.
    """

    # Modify the existing DB award to now have an `action_date` before 2007-10-01
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2004-01-15",
        update_date=datetime.now(),
    )

    recently_modified_awards_before_fy2008 = _check_awards_for_pre_fy2008(spark=None)

    assert len(recently_modified_awards_before_fy2008) == 1


@pytest.mark.django_db(transaction=True)
def test_find_modified_awards_before_fy2008_spark(spark, award_search_data_fixture):
    """Test that we can find any awards that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01), but have
    have been recently updated to have an `action_date` before FY2008 now.
    """

    # Modify the existing DB award to now have an `action_date` before 2007-10-01
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2004-01-15",
        update_date=datetime.now(),
    )

    recently_modified_awards_before_fy2008 = _check_awards_for_pre_fy2008(spark)

    assert len(recently_modified_awards_before_fy2008) == 1
