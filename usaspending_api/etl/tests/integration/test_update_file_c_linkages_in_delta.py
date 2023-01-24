from django.core.management import call_command
from pytest import mark

from usaspending_api.common.helpers.spark_helpers import load_dict_to_delta_table

common_award_info = {
    "generated_unique_award_id": "fake_id",
    "is_fpds": False,
    "subaward_count": 0,
    "transaction_unique_id": "fake_id",
}

common_faba_info = {"submission_id": "0", "distinct_award_key": "fake_key"}

award_records = [
    {
        "id": 1,
    },
    {"id": 2, "piid": "piid_1", "parent_award_piid": "parent_piid_1"},
]

faba_records = [
    # No Award to link to
    {"financial_accounts_by_awards_id": 1, "expected_award_id": None},
    # Starts with an Award that no longer exists and unlinks
    {"financial_accounts_by_awards_id": 2, "award_id": "200", "expected_award_id": None},
    # Links to an Award based on PIID and Parent PIID
    {
        "financial_accounts_by_awards_id": 3,
        "piid": "piid_1",
        "parent_award_id": "parent_piid_1",
        "expected_award_id": 2,
    },
]


def read_output_faba_to_award_id(spark):
    results = spark.sql(
        """
    SELECT
        financial_accounts_by_awards_id,
        award_id
    FROM
        int.financial_accounts_by_awards
    """
    )

    output_faba_to_award_id = {}
    for item in results.collect():
        output_faba_to_award_id[item[0]] = item[1]

    return output_faba_to_award_id


@mark.django_db(transaction=True)
def test_update_file_c_linkages_in_delta(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):

    # Add common (required) fields to dictionaries before loading them to delta
    full_award_records = [dict(item, **common_award_info) for item in award_records]

    expected_faba_to_award_id = {}
    for item in faba_records:
        expected_faba_to_award_id[item["financial_accounts_by_awards_id"]] = item.pop("expected_award_id")

    full_faba_records = [dict(item, **common_faba_info) for item in faba_records]

    # Make sure the table has been created first
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "int", "awards", full_award_records, True)
    load_dict_to_delta_table(
        spark, s3_unittest_data_bucket, "raw", "financial_accounts_by_awards", full_faba_records, True
    )

    call_command("update_file_c_linkages_in_delta", "--no-clone", "--spark-s3-bucket", s3_unittest_data_bucket)

    output_faba_to_award_id = read_output_faba_to_award_id(spark)
    for faba_id, expected_award_id in expected_faba_to_award_id.items():
        if expected_award_id is None:
            assert output_faba_to_award_id[faba_id] is None
        else:
            assert output_faba_to_award_id[faba_id] == expected_award_id
