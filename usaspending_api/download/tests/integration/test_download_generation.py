import re

import pytest
from django.db.models import F, Value

from usaspending_api.common.helpers.orm_helpers import ConcatAll, generate_raw_quoted_query
from usaspending_api.download.filestreaming.download_generation import apply_annotations_to_sql
from usaspending_api.search.models import AwardSearch, TransactionSearch


@pytest.mark.django_db
def test_apply_annotations_to_sql_single_table():
    selected_columns = ["transaction_id", "action_type_code", "action_type", "date_range"]
    queryset = (
        TransactionSearch.objects.values("transaction_id")
        .annotate(
            action_type_code=F("action_type"),
            action_type=F("action_type_description"),
            date_range=ConcatAll(
                F("period_of_performance_start_date"), Value(" - "), F("period_of_performance_current_end_date")
            ),
        )
        .values(*selected_columns)
        .order_by("transaction_id")
    )
    queryset = queryset[:1]
    raw_query = generate_raw_quoted_query(queryset)
    result_query = apply_annotations_to_sql(raw_query, selected_columns)
    expected_query = (
        "SELECT "
        '"transaction_search"."transaction_id" AS "transaction_id", '
        '"transaction_search"."action_type" AS "action_type_code", '
        '"transaction_search"."action_type_description" AS "action_type", '
        "CONCAT("
        '"transaction_search"."period_of_performance_start_date", '
        "' - ', "
        '"transaction_search"."period_of_performance_current_end_date"'
        ') AS "date_range" '
        'FROM "transaction_search" '
        "ORDER BY 1 ASC LIMIT 1"
    )
    assert result_query == expected_query


@pytest.mark.django_db
def test_apply_annotations_to_sql_joined_table():
    select_columns = [
        "award_id",
        "earliest_transaction_search__solicitation_date",
        "latest_transaction_search__action_date",
    ]
    annotate_columns = ["assistance_award_unique_key", "award_id_fain", "date_range"]
    queryset = (
        AwardSearch.objects.values(*select_columns)
        .annotate(
            assistance_award_unique_key=F("generated_unique_award_id"),
            award_id_fain=F("fain"),
            date_range=ConcatAll(
                F("latest_transaction_search__period_of_performance_start_date"),
                Value(" - "),
                F("latest_transaction_search__period_of_performance_current_end_date"),
            ),
        )
        .values(*select_columns, *annotate_columns)
        .order_by("award_id")
    )
    queryset = queryset[:1]
    raw_query = generate_raw_quoted_query(queryset)
    result_query = apply_annotations_to_sql(raw_query, [*select_columns, *annotate_columns])

    # The second reference to the "transaction_search" table should be aliases. Instead of hard coding this value
    # it is retrieved via a regex in case it happens to change.
    table_alias = re.search(r'"transaction_search" ([^ ]*) ON', raw_query).group(1)

    expected_query = (
        "SELECT "
        '"award_search"."award_id" AS "award_id", '
        '"transaction_search"."solicitation_date" AS "earliest_transaction_search__solicitation_date", '
        f'{table_alias}."action_date" AS "latest_transaction_search__action_date", '
        '"award_search"."generated_unique_award_id" AS "assistance_award_unique_key", '
        '"award_search"."fain" AS "award_id_fain", '
        "CONCAT("
        f'{table_alias}."period_of_performance_start_date", '
        "' - ', "
        f'{table_alias}."period_of_performance_current_end_date"'
        ') AS "date_range" '
        'FROM "award_search" '
        'LEFT OUTER JOIN "transaction_search" ON '
        '("award_search"."earliest_transaction_search_id" = "transaction_search"."transaction_id") '
        f'LEFT OUTER JOIN "transaction_search" {table_alias} '
        'ON ("award_search"."latest_transaction_search_id" = T4."transaction_id") '
        "ORDER BY 1 ASC LIMIT 1"
    )
    assert result_query == expected_query
