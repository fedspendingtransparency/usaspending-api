from django.core.management import call_command
from pytest import mark

from usaspending_api.common.helpers.spark_helpers import load_dict_to_delta_table

common_award_info = {
    "generated_unique_award_id": "fake_id",
    "is_fpds": False,
    "subaward_count": 0,
    "transaction_unique_id": "fake_id"
}

common_faba_info = {
    "submission_id": "0",
    "distinct_award_key": "fake_key"
}

award_records = [
    {
        "id": 1,
    }
]

faba_records = [
    {
        "financial_accounts_by_awards_id": "1"
    }
]


def read_faba_record(spark, faba_id):
    result = spark.sql(f"""
    SELECT
        award_id
    FROM
        int.financial_accounts_by_awards_id
    WHERE
        financial_accounts_by_awards_id = {faba_id}
    """)

    assert len(result.collect()) < 2

    return result.collect()[0][0]


@mark.django_db(transaction=True)
def test_update_file_c_linkages_in_delta(
    spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):

    # Add common (required) fields to dictionaries before loading them to delta
    full_award_records = [dict(item, **common_award_info) for item in award_records]
    full_faba_records = [dict(item, **common_faba_info) for item in faba_records]

    # Make sure the table has been created first
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "raw", "awards", full_award_records)
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "raw", "financial_accounts_by_awards", full_faba_records)

    call_command("update_file_c_linkages_in_delta")

    faba_1_award_id = read_faba_record(1)

    assert faba_1_award_id is None
