from datetime import datetime
from django.core.management import call_command
from pytest import mark

from usaspending_api.common.helpers.spark_helpers import load_dict_to_delta_table

starting_update_date = datetime.utcfromtimestamp(0)

common_award_info = {
    "generated_unique_award_id": "fake_id",
    "is_fpds": False,
    "subaward_count": 0,
    "transaction_unique_id": "fake_id",
    "update_date": starting_update_date,
}

common_faba_info = {"submission_id": "0", "distinct_award_key": "fake_key"}

award_records = [
    {"id": 1, "expected_update": False},
    {"id": 2, "piid": "piid_1", "parent_award_piid": "parent_piid_1", "expected_update": True},
    {"id": 3, "piid": "piid_2", "parent_award_piid": "parent_piid_2", "expected_update": False},
    {"id": 4, "piid": "piid_2", "parent_award_piid": "parent_piid_2", "expected_update": False},
    {"id": 5, "piid": "piid_3", "expected_update": True},
    {"id": 6, "piid": "piid_4", "expected_update": False},
    {"id": 7, "piid": "piid_4", "expected_update": False},
    {"id": 8, "piid": "piid_5", "parent_award_piid": "parent_piid_5", "expected_update": False},
    {"id": 9, "fain": "fain_1", "uri": "uri_1", "expected_update": True},
    {"id": 10, "fain": "fain_2", "uri": "uri_2", "expected_update": False},
    {"id": 11, "fain": "fain_2", "uri": "uri_2", "expected_update": False},
    {"id": 12, "expected_update": True},
]

faba_records = [
    # No Award to link to
    {"financial_accounts_by_awards_id": 1, "expected_award_id": None},
    # Starts with an Award that no longer exists and unlinks
    {"financial_accounts_by_awards_id": 2, "award_id": 200, "expected_award_id": None},
    # Unlinks from deleted Award then links to an Award based on PIID and Parent PIID
    {
        "financial_accounts_by_awards_id": 3,
        "piid": "piid_1",
        "parent_award_id": "parent_piid_1",
        "award_id": 200,
        "expected_award_id": 2,
    },
    # Matches two Awards on PIID and Parent PIID - No Update
    {
        "financial_accounts_by_awards_id": 4,
        "piid": "piid_2",
        "parent_award_id": "parent_piid_2",
        "expected_award_id": None,
    },
    # Matches a single Award (2) based on PIID and Parent PIID, but already linked
    {
        "financial_accounts_by_awards_id": 5,
        "piid": "piid_1",
        "parent_award_id": "parent_piid_1",
        "award_id": 1,
        "expected_award_id": 1,
    },
    # Links to an Award on PIID and Parent PIID is Null
    {
        "financial_accounts_by_awards_id": 6,
        "piid": "piid_3",
        "expected_award_id": 5,
    },
    # Matches two Awards on PIID with Parent PIID NULL - No Update
    {
        "financial_accounts_by_awards_id": 7,
        "piid": "piid_4",
        "parent_award_id": None,
        "expected_award_id": None,
    },
    # Matches on PIID but not Parent PIID - No Update
    {
        "financial_accounts_by_awards_id": 8,
        "piid": "piid_5",
        "parent_award_id": "piid_4",
        "expected_award_id": None,
    },
    # Matches a single Award (5) based on PIID with Parent ID NULL, but already linked
    {
        "financial_accounts_by_awards_id": 9,
        "piid": "piid_3",
        "award_id": 1,
        "expected_award_id": 1,
    },
    # Links to an Award based on FAIN with URI NULL
    {
        "financial_accounts_by_awards_id": 10,
        "fain": "fain_1",
        "expected_award_id": 9,
    },
    # Linkes to an Award based on FAIN but not URI
    {
        "financial_accounts_by_awards_id": 11,
        "fain": "fain_1",
        "uri": "uri_2",
        "expected_award_id": 9,
    },
    # Matches an Award based on FAIN with URI NULL, but already linked
    {
        "financial_accounts_by_awards_id": 12,
        "fain": "fain_1",
        "award_id": 1,
        "expected_award_id": 1,
    },
    # Matches two Awards on FAIN with URI NULL - No Update
    {
        "financial_accounts_by_awards_id": 13,
        "fain": "fain_2",
        "expected_award_id": None,
    },
    # Links to an Award based on URI with FAIN NULL
    {
        "financial_accounts_by_awards_id": 14,
        "uri": "uri_1",
        "expected_award_id": 9,
    },
    # Links to an Award based on URI but not FAIN
    {
        "financial_accounts_by_awards_id": 15,
        "fain": "fain_2",
        "uri": "uri_1",
        "expected_award_id": 9,
    },
    # Matches an Award based on URI with FAIN NULL, but already linked
    {
        "financial_accounts_by_awards_id": 16,
        "uri": "uri_1",
        "award_id": 1,
        "expected_award_id": 1,
    },
    # Matches two Awards on URI with FAIN NULL - No Update
    {
        "financial_accounts_by_awards_id": 17,
        "uri": "uri_2",
        "expected_award_id": None,
    },
    # Links to an Award based on FAIN when both FAIN/URI are populated
    {"financial_accounts_by_awards_id": 18, "fain": "fain_1", "uri": "uri_200", "expected_award_id": 9},
    # Matches an Award based on FAIN when both FAIN/URI are populated, but already linked
    {"financial_accounts_by_awards_id": 19, "fain": "fain_1", "uri": "uri_200", "award_id": 1, "expected_award_id": 1},
    # Matches two Awards based on FAIN when both FAIN/URI are populated - No Update
    {"financial_accounts_by_awards_id": 20, "fain": "fain_2", "uri": "uri_200", "expected_award_id": None},
    # Links to an Award based on URI when both FAIN/URI are populated
    {"financial_accounts_by_awards_id": 21, "fain": "fain_200", "uri": "uri_1", "expected_award_id": 9},
    # Matches an Award based on URI when both FAIN/URI are populated, but already linked
    {"financial_accounts_by_awards_id": 22, "fain": "fain_200", "uri": "uri_1", "award_id": 1, "expected_award_id": 1},
    # Matches two Awards based on URI when both FAIN/URI are populated - No Update
    {"financial_accounts_by_awards_id": 23, "fain": "fain_200", "uri": "uri_2", "expected_award_id": None},
    # Test data for testing submission deletions
    {
        "financial_accounts_by_awards_id": 24,
        "award_id": 12,
        "expected_award_id": 1,
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


def read_updated_award_ids(spark):
    results = spark.sql(
        f"""
    SELECT
        id, update_date
    FROM
        int.awards
    WHERE
        update_date > '{starting_update_date.isoformat()}'
    """
    )

    return [item["id"] for item in results.collect()]


@mark.django_db(transaction=True)
def test_update_file_c_linkages_in_delta(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):

    # Build list of award ids expected to be updated, and add common (required)
    # fields to Awards before loading them to delta
    full_award_records = [dict(item, **common_award_info) for item in award_records]
    expected_updated_award_ids = []
    for item in award_records:
        if item.pop("expected_update"):
            expected_updated_award_ids.append(item["id"])

    # Build expected FABA to Award ID mapping and add common fields to FABA
    expected_faba_to_award_id = {}
    full_raw_faba_records = []
    for record in faba_records:
        # award_id 1 is the award belonging to the submission we are "deleting"
        #   so don't let the faba record be present in the faba records list
        if "award_id" in record and record["financial_accounts_by_awards_id"] == 24:
            continue
        full_raw_faba_records.append(dict(record, **common_faba_info))
        expected_faba_to_award_id[record["financial_accounts_by_awards_id"]] = record.pop("expected_award_id")

    # We need to populate the int.financial_accounts_by_awards to test submission deletion logic
    full_int_faba_records = [dict(record, **common_faba_info) for record in faba_records]

    # Make sure the table has been created first
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "int", "awards", full_award_records, True)
    load_dict_to_delta_table(
        spark, s3_unittest_data_bucket, "raw", "financial_accounts_by_awards", full_raw_faba_records, True
    )
    load_dict_to_delta_table(
        spark, s3_unittest_data_bucket, "int", "financial_accounts_by_awards", full_int_faba_records, True
    )

    call_command("update_file_c_linkages_in_delta", "--no-clone", "--spark-s3-bucket", s3_unittest_data_bucket)

    # Verify mapping of FABA->Awards matches the expected results
    output_faba_to_award_id = read_output_faba_to_award_id(spark)
    for faba_id, expected_award_id in expected_faba_to_award_id.items():
        if expected_award_id is None:
            assert output_faba_to_award_id[faba_id] is None, f"Failed FABA ID: {faba_id}"
        else:
            assert output_faba_to_award_id[faba_id] == expected_award_id, f"Failed FABA ID: {faba_id}"

    # Verify that newly linked Awards have an updated update_date
    updated_award_ids = read_updated_award_ids(spark)
    assert sorted(updated_award_ids) == sorted(expected_updated_award_ids)
