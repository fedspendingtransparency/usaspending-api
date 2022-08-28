from pytest import mark

from usaspending_api.etl.tests.integration.test_load_to_from_delta import (
    create_and_load_all_delta_tables,
    verify_delta_query_loaded_to_delta,
)


@mark.django_db(transaction=True)
def test_load_query_to_delta_for_sam_recipient(
    spark,
    s3_unittest_data_bucket,
    broker_server_dblink_setup,
    populate_broker_data_sam_recipient_to_delta,
    populate_data_for_transaction_search,
    hive_unittest_metastore_db,
):
    tables_to_load = [
        "awards",
        "sam_recipient",
        "transaction_normalized",
        "transaction_fabs",
        "transaction_fpds",
        "recipient_lookup",
        "financial_accounts_by_awards",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_query_loaded_to_delta(spark, "sam_recipient", s3_unittest_data_bucket)

    expected_dummy_data = [
        {
            "awardee_or_recipient_uniqu": "Recipient_id_1",
            "legal_business_name": "First Recipient",
            "ultimate_parent_unique_ide": 34567822,
            "broker_duns_id": 34567822,
            "update_date": "2019-06-12 12:45:58.789296",
            "address_line_1": "ADDRESS_LINE_1",
            "address_line_2": "ADDRESS_LINE_2",
            "city": "CITY_NAME",
            "congressional_district": "52",
            "country_code": "USA",
            "state": "DC",
            "zip": "02214",
            "zip4": "0987",
            "business_types_codes": ["BTC_1", "BTC_2"],
            "dba_name": None,
            "entity_structure": "A_Business",
            "uei": "009-890-124256",
            "ultimate_parent_uei": "009-890-124256",
        }
    ]
    verify_delta_query_loaded_to_delta(
        spark,
        "sam_recipient",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        dummy_data=expected_dummy_data,
    )
