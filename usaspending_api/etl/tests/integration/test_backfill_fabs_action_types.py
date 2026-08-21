"""Integration tests for the backfill_fabs_action_types one-time command.

NOTE: Uses Pytest Fixtures from usaspending_api/etl/tests/conftest.py
"""

from datetime import datetime, timezone

from django.core.management import call_command
from model_bakery import baker
from pytest import mark

from usaspending_api.etl.tests.integration.test_load_to_from_delta import load_delta_table_from_postgres

_ACTION_DATE = datetime(2022, 10, 31, tzinfo=timezone.utc)

# Each tuple is: (
#   published_fabs_id,
#   old action_type,
#   federal_action_obligation,
#   original_loan_subsidy_cost,
#   expected new action_type,
#   expected new action_type_description
# )
_MAPPING_CASES = [
    (1, "A", 100, None, "A1", "New Award"),
    (2, "B", 100, None, "B1", "Continuation"),
    (3, "C", 0, None, "EX", "Other Action, Non-Financial"),
    (4, "C", 100, 100, "FX", "Other Action, Financial"),
    (5, "D", 100, None, "FX", "Other Action, Financial"),
    (6, "E", 100, None, "G1", "Mixed Aggregate"),
    (7, "0", 100, None, "0", "Unmapped Description"),
    (8, None, 100, None, None, "Unmapped Description"),
    # New (post-GSDM 1.2) code that doesn't match any historic mapping should pass through unchanged.
    (9, "C1", 100, None, "C1", "Termination Initiated: Material Failure to Comply"),
]

_HISTORIC_MAPPED_CODES = ("A", "B", "C", "D", "E")


def _initial_description(action_type, expected_desc):
    # Codes that are actually part of the historic mapping (A, B, C, D, E) are seeded with a
    # deliberately "wrong" (old) description to prove the backfill actually rewrites them.
    # Everything else (blank/'0'/NULL, or new post-GSDM-1.2 codes like "C1") arrives already
    # carrying its final description, since the backfill should leave it untouched.
    return "Old Description" if action_type in _HISTORIC_MAPPED_CODES else expected_desc


def _make_source_assistance_transactions():
    for published_fabs_id, action_type, foa, olsc, _, expected_desc in _MAPPING_CASES:
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=published_fabs_id,
            afa_generated_unique=f"backfill_test_{published_fabs_id}",
            action_date=_ACTION_DATE.isoformat(),
            action_type=action_type,
            action_type_description=_initial_description(action_type, expected_desc),
            federal_action_obligation=foa,
            original_loan_subsidy_cost=olsc,
            created_at=_ACTION_DATE,
            updated_at=_ACTION_DATE,
            is_active=True,
            unique_award_key=f"backfill_test_award_{published_fabs_id}",
        )


def _make_transaction_search_rows():
    for published_fabs_id, action_type, foa, olsc, _, expected_desc in _MAPPING_CASES:
        baker.make(
            "search.TransactionSearch",
            transaction_id=published_fabs_id,
            is_fpds=False,
            transaction_unique_id=f"backfill_test_{published_fabs_id}",
            action_type=action_type,
            action_type_description=_initial_description(action_type, expected_desc),
            federal_action_obligation=foa,
            original_loan_subsidy_cost=olsc,
        )


@mark.django_db(transaction=True)
def test_backfill_updates_postgres_tables():
    _make_source_assistance_transactions()
    _make_transaction_search_rows()

    call_command("backfill_fabs_action_types", "--postgres-only")

    from usaspending_api.search.models import TransactionSearch
    from usaspending_api.transactions.models.source_assistance_transaction import SourceAssistanceTransaction

    for published_fabs_id, _, _, _, expected_type, expected_desc in _MAPPING_CASES:
        sat = SourceAssistanceTransaction.objects.get(published_fabs_id=published_fabs_id)
        assert sat.action_type == expected_type
        assert sat.action_type_description == expected_desc

        ts = TransactionSearch.objects.get(transaction_id=published_fabs_id)
        assert ts.action_type == expected_type
        assert ts.action_type_description == expected_desc


@mark.django_db(transaction=True)
def test_backfill_postgres_dry_run_makes_no_changes():
    _make_source_assistance_transactions()

    call_command("backfill_fabs_action_types", "--postgres-only", "--dry-run")

    from usaspending_api.transactions.models.source_assistance_transaction import SourceAssistanceTransaction

    for published_fabs_id, action_type, _, _, _, _ in _MAPPING_CASES:
        sat = SourceAssistanceTransaction.objects.get(published_fabs_id=published_fabs_id)
        assert sat.action_type == action_type


@mark.django_db(transaction=True)
def test_backfill_updates_published_fabs_in_delta(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _make_source_assistance_transactions()

    # Load raw.published_fabs (Delta) from the seeded source_assistance_transaction (Postgres), mirroring the
    # real bronze-layer load pipeline.
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    call_command(
        "create_delta_table", f"--spark-s3-bucket={s3_unittest_data_bucket}", "--destination-table=transaction_fabs"
    )
    call_command(
        "create_delta_table",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
        "--destination-table=transaction_normalized",
    )

    call_command("backfill_fabs_action_types", "--delta-only")

    rows = {
        row["published_fabs_id"]: row
        for row in spark.sql(
            "SELECT published_fabs_id, action_type, action_type_description FROM raw.published_fabs"
        ).collect()
    }
    for published_fabs_id, _, _, _, expected_type, expected_desc in _MAPPING_CASES:
        row = rows[published_fabs_id]
        assert row["action_type"] == expected_type
        assert row["action_type_description"] == expected_desc
