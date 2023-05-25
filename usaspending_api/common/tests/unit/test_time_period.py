from usaspending_api.common.filters.time_period import TransactionSearchTimePeriod
from usaspending_api.common.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.elasticsearch.filter import _QueryType


def test_transaction_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    transaction_search = TransactionSearchTimePeriod(
        default_start_date=default_start_date, default_end_date=default_end_date
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
    assert transaction_search.gte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_gte_date_type = "action_date"
    expected_lte_date_type = "action_date"
    assert transaction_search.gte_date_type() == expected_gte_date_type
    assert transaction_search.gte_date_type() == expected_lte_date_type

    # Repeating all tests with new_awards_only true
    time_period_filter = {"new_awards_only": True, "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    assert transaction_search.start_date() == expected_start_date
    assert transaction_search.end_date() == expected_end_date

    time_period_filter = {
        "new_awards_only": True,
        "date_type": "date_signed",
        "start_date": "2020-10-01",
        "end_date": "2021-09-30",
    }
    transaction_search.filter_value = time_period_filter
    expected_gte_date_type = "award_date_signed"
    expected_lte_date_type = "award_date_signed"
    assert transaction_search.gte_date_type() == expected_gte_date_type
    assert transaction_search.gte_date_type() == expected_lte_date_type


def test_new_awards_only_transaction_search_time_period():
    default_start_date = "1111-11-11"
    default_end_date = "9999-99-99"
    transaction_search = TransactionSearchTimePeriod(
        default_start_date=default_start_date, default_end_date=default_end_date
    )
    transaction_search_decorator = NewAwardsOnlyTimePeriod(
        transaction_search_time_period_obj=transaction_search, query_type=_QueryType.TRANSACTIONS
    )

    # Testing for correct output of start and end date
    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_new_awards_only = None
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
    assert transaction_search_decorator.gte_date_type() == expected_lte_date_type

    time_period_filter = {"start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_gte_date_type = "action_date"
    expected_lte_date_type = "action_date"
    assert transaction_search_decorator.gte_date_type() == expected_gte_date_type
    assert transaction_search_decorator.gte_date_type() == expected_lte_date_type

    # Repeating all tests with new_awards_only true
    time_period_filter = {"new_awards_only": True, "start_date": "2020-10-01", "end_date": "2021-09-30"}
    transaction_search_decorator.filter_value = time_period_filter
    expected_start_date = "2020-10-01"
    expected_end_date = "2021-09-30"
    expected_new_awards_only = False
    assert transaction_search_decorator.start_date() == expected_start_date
    assert transaction_search_decorator.end_date() == expected_end_date
    assert transaction_search_decorator._new_awards_only() == expected_new_awards_only

    time_period_filter = {
        "new_awards_only": True,
        "date_type": "date_signed",
        "start_date": "2020-10-01",
        "end_date": "2021-09-30",
    }
    transaction_search_decorator.filter_value = time_period_filter
    expected_gte_date_type = "award_date_signed"
    expected_lte_date_type = "award_date_signed"
    expected_new_awards_only = True
    assert transaction_search_decorator.gte_date_type() == expected_gte_date_type
    assert transaction_search_decorator.gte_date_type() == expected_lte_date_type
    assert transaction_search_decorator._new_awards_only() == expected_new_awards_only
