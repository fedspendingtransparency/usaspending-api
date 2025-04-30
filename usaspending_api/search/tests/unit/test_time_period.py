from usaspending_api.search.filters.time_period.query_types import (
    AwardSearchTimePeriod,
    SubawardSearchTimePeriod,
    TransactionSearchTimePeriod,
)
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.elasticsearch.filter import QueryType


def test_transaction_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    transaction_search = TransactionSearchTimePeriod(
        default_start_date=default_start_date,
        default_end_date=default_end_date,
    )

    # Testing for correct output of start and end date
    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    assert transaction_search.start_date() == expected_start_date
    assert transaction_search.end_date() == expected_end_date

    time_period_filter = {"end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    assert transaction_search.start_date() == default_start_date

    time_period_filter = {"start_date": "2020-10-01"}
    transaction_search.filter_value = time_period_filter
    assert transaction_search.end_date() == default_end_date

    time_period_filter = {}
    transaction_search.filter_value = time_period_filter
    assert transaction_search.end_date() == default_end_date
    assert transaction_search.start_date() == default_start_date

    # Testing for correct output of date type
    time_period_filter = {"date_type": "date_signed", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_gte_date_type = "award_date_signed"
    expected_lte_date_type = "award_date_signed"
    assert transaction_search.gte_date_type() == expected_gte_date_type
    assert transaction_search.lte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_gte_date_type = "action_date"
    expected_lte_date_type = "action_date"
    assert transaction_search.gte_date_type() == expected_gte_date_type
    assert transaction_search.lte_date_type() == expected_lte_date_type

    # Repeating all tests with new_awards_only true
    time_period_filter = {"date_type": "new_awards_only", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    assert transaction_search.start_date() == expected_start_date
    assert transaction_search.end_date() == expected_end_date

    time_period_filter = {
        "date_type": "new_awards_only",
        "start_date": "2020-10-01",
        "end_date": "2021-09-30",
    }
    transaction_search.filter_value = time_period_filter
    expected_gte_date_type = "new_awards_only"
    expected_lte_date_type = "new_awards_only"
    assert transaction_search.gte_date_type() == expected_gte_date_type
    assert transaction_search.lte_date_type() == expected_lte_date_type


def test_new_awards_only_transaction_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    transaction_search = TransactionSearchTimePeriod(
        default_start_date=default_start_date, default_end_date=default_end_date
    )
    transaction_search_decorator = NewAwardsOnlyTimePeriod(
        time_period_obj=transaction_search, query_type=QueryType.TRANSACTIONS
    )

    # Testing for correct output of start and end date
    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_new_awards_only = False
    assert transaction_search_decorator.start_date() == expected_start_date
    assert transaction_search_decorator.end_date() == expected_end_date
    assert transaction_search_decorator._new_awards_only() == expected_new_awards_only

    time_period_filter = {"end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    assert transaction_search_decorator.start_date() == default_start_date

    time_period_filter = {"start_date": "2020-10-01"}
    transaction_search_decorator.filter_value = time_period_filter
    assert transaction_search_decorator.end_date() == default_end_date

    time_period_filter = {}
    transaction_search_decorator.filter_value = time_period_filter
    assert transaction_search_decorator.end_date() == default_end_date
    assert transaction_search_decorator.start_date() == default_start_date

    # Testing for correct output of date type
    time_period_filter = {"date_type": "date_signed", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_gte_date_type = "award_date_signed"
    expected_lte_date_type = "award_date_signed"
    assert transaction_search_decorator.gte_date_type() == expected_gte_date_type
    assert transaction_search_decorator.lte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_gte_date_type = "action_date"
    expected_lte_date_type = "action_date"
    assert transaction_search_decorator.gte_date_type() == expected_gte_date_type
    assert transaction_search_decorator.lte_date_type() == expected_lte_date_type

    # Repeating all tests with new_awards_only true
    time_period_filter = {"date_type": "new_awards_only", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_new_awards_only = True
    assert transaction_search_decorator.start_date() == expected_start_date
    assert transaction_search_decorator.end_date() == expected_end_date
    assert transaction_search_decorator._new_awards_only() == expected_new_awards_only


def test_new_awards_only_award_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    search_obj = AwardSearchTimePeriod(default_start_date=default_start_date, default_end_date=default_end_date)
    search_obj_decorator = NewAwardsOnlyTimePeriod(time_period_obj=search_obj, query_type=QueryType.AWARDS)

    # Testing for correct output of start and end date
    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    search_obj_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_new_awards_only = False
    assert search_obj_decorator.start_date() == expected_start_date
    assert search_obj_decorator.end_date() == expected_end_date
    assert search_obj_decorator._new_awards_only() == expected_new_awards_only

    time_period_filter = {"end_date": "2021-09-30"}
    search_obj_decorator.filter_value = time_period_filter
    assert search_obj_decorator.start_date() == default_start_date

    time_period_filter = {"start_date": "2020-10-01"}
    search_obj_decorator.filter_value = time_period_filter
    assert search_obj_decorator.end_date() == default_end_date

    time_period_filter = {}
    search_obj_decorator.filter_value = time_period_filter
    assert search_obj_decorator.end_date() == default_end_date
    assert search_obj_decorator.start_date() == default_start_date

    # Testing for correct output of date type
    time_period_filter = {"date_type": "date_signed", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    search_obj_decorator.filter_value = time_period_filter
    expected_gte_date_type = "date_signed"
    expected_lte_date_type = "date_signed"
    assert search_obj_decorator.gte_date_type() == expected_gte_date_type
    assert search_obj_decorator.lte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    search_obj_decorator.filter_value = time_period_filter
    expected_gte_date_type = "action_date"
    expected_lte_date_type = "date_signed"
    assert search_obj_decorator.gte_date_type() == expected_gte_date_type
    assert search_obj_decorator.lte_date_type() == expected_lte_date_type

    time_period_filter = {"date_type": "new_awards_only", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    search_obj_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_gte_date_type_range = [{"date_signed": {"gte": f"{expected_start_date}"}}]
    expected_lte_date_type_range = [{"date_signed": {"lte": f"{expected_end_date}"}}]
    expected_new_awards_only = True
    assert search_obj_decorator.start_date() == expected_start_date
    assert search_obj_decorator.end_date() == expected_end_date
    assert search_obj_decorator.gte_date_range() == expected_gte_date_type_range
    assert search_obj_decorator.lte_date_range() == expected_lte_date_type_range
    assert search_obj_decorator._new_awards_only() == expected_new_awards_only


def test_subaward_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    time_period_obj = SubawardSearchTimePeriod(
        default_start_date=default_start_date,
        default_end_date=default_end_date,
    )

    # Testing for correct output of start and end date
    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    time_period_obj.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    assert time_period_obj.start_date() == expected_start_date
    assert time_period_obj.end_date() == expected_end_date

    time_period_filter = {"end_date": "2021-09-30"}
    time_period_obj.filter_value = time_period_filter
    assert time_period_obj.start_date() == default_start_date

    time_period_filter = {"start_date": "2020-10-01"}
    time_period_obj.filter_value = time_period_filter
    assert time_period_obj.end_date() == default_end_date

    time_period_filter = {}
    time_period_obj.filter_value = time_period_filter
    assert time_period_obj.end_date() == default_end_date
    assert time_period_obj.start_date() == default_start_date

    # Testing for correct output of date type
    time_period_filter = {"date_type": "date_signed", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    time_period_obj.filter_value = time_period_filter
    expected_gte_date_type = "date_signed"
    expected_lte_date_type = "date_signed"
    assert time_period_obj.gte_date_type() == expected_gte_date_type
    assert time_period_obj.lte_date_type() == expected_lte_date_type

    time_period_filter = {"date_type": "last_modified_date", "start_date": "2020-10-01", "end_date": "2021-09-30"}
    time_period_obj.filter_value = time_period_filter
    expected_gte_date_type = "last_modified_date"
    expected_lte_date_type = "last_modified_date"
    assert time_period_obj.gte_date_type() == expected_gte_date_type
    assert time_period_obj.lte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    time_period_obj.filter_value = time_period_filter
    expected_gte_date_type = "sub_action_date"
    expected_lte_date_type = "sub_action_date"
    assert time_period_obj.gte_date_type() == expected_gte_date_type
    assert time_period_obj.lte_date_type() == expected_lte_date_type
