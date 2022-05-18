"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from django.core.management import call_command


def test_create_sam_recipient_delta_table(spark, s3_unittest_data_bucket):
    call_command(
        "create_delta_table",
        "--destination-table=sam_recipient",
        "--config",
        f"AWS_S3_BUCKET={s3_unittest_data_bucket}"  # TODO: This doesn't seem to work as designed.
    )
