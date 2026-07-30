import pytest
from django.db.models import F, Value
from model_bakery import baker

from usaspending_api.common.helpers.orm_helpers import ConcatAll, generate_raw_quoted_query
from usaspending_api.download.filestreaming.download_generation import apply_annotations_to_sql
from usaspending_api.search.models import TransactionSearch


@pytest.mark.django_db
def test_apply_annotations_to_sql():
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
