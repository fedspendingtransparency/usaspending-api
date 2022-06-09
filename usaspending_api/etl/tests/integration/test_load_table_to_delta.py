"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import json

from datetime import datetime
from typing import List, Dict, Any

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import fixture, mark

from django.core.management import call_command

from usaspending_api.common.helpers.spark_helpers import create_ref_temp_views
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


@fixture
def populate_data_for_transaction_search():
    # Create recipient data for two transactions; the other two will generate ad hoc
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        legal_business_name="FABS TEST RECIPIENT",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        recipient_level="R",
        recipient_name="FABS TEST RECIPIENT",
        _fill_optional=True,
    )
    baker.make(
        "recipient.DUNS",
        broker_duns_id="1",
        uei="FABSUEI12345",
        legal_business_name="FABS TEST RECIPIENT",
        _fill_optional=True,
    )

    # Create agency data
    funding_toptier_agency = baker.make("references.ToptierAgency", _fill_optional=True)
    funding_subtier_agency = baker.make("references.SubtierAgency", _fill_optional=True)
    funding_agency = baker.make(
        "references.Agency",
        toptier_agency=funding_toptier_agency,
        subtier_agency=funding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    awarding_toptier_agency = baker.make("references.ToptierAgency", _fill_optional=True)
    awarding_subtier_agency = baker.make("references.SubtierAgency", _fill_optional=True)
    awarding_agency = baker.make(
        "references.Agency",
        toptier_agency=awarding_toptier_agency,
        subtier_agency=awarding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    # Create reference data
    baker.make("references.NAICS", code="123456", _fill_optional=True)
    baker.make("references.PSC", code="12", _fill_optional=True)
    baker.make("references.Cfda", program_number="12.456", _fill_optional=True)
    baker.make("references.CityCountyStateCode", state_alpha="VA", county_numeric="001", _fill_optional=True)
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES", _fill_optional=True)
    baker.make("recipient.StateData", code="VA", name="Virginia", fips="51", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="000", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="001", _fill_optional=True)
    baker.make("references.PopCongressionalDistrict", state_code="51", congressional_district="01")
    defc_l = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19", _fill_optional=True)
    defc_m = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19", _fill_optional=True)
    defc_q = baker.make("references.DisasterEmergencyFundCode", code="Q", group_name=None, _fill_optional=True)

    # Create awards and transactions
    asst_award = baker.make("awards.Award", type="07")
    cont_award = baker.make("awards.Award", type="A")

    asst_trx1 = baker.make(
        "awards.TransactionNormalized",
        action_date="2021-01-01",
        award=asst_award,
        is_fpds=False,
        type="07",
        awarding_agency=awarding_agency,
        funding_agency=funding_agency,
        _fill_optional=True,
    )
    asst_trx2 = baker.make(
        "awards.TransactionNormalized",
        action_date="2021-04-01",
        award=asst_award,
        is_fpds=False,
        type="07",
        awarding_agency=awarding_agency,
        funding_agency=funding_agency,
        _fill_optional=True,
    )
    cont_trx1 = baker.make(
        "awards.TransactionNormalized",
        action_date="2021-07-01",
        award=cont_award,
        is_fpds=True,
        type="A",
        awarding_agency=awarding_agency,
        funding_agency=funding_agency,
        _fill_optional=True,
    )
    cont_trx2 = baker.make(
        "awards.TransactionNormalized",
        action_date="2021-10-01",
        award=cont_award,
        is_fpds=True,
        type="A",
        awarding_agency=awarding_agency,
        funding_agency=funding_agency,
        _fill_optional=True,
    )

    baker.make(
        "awards.TransactionFABS",
        transaction=asst_trx1,
        cfda_number="12.456",
        action_date="2021-01-01",
        uei="FABSUEI12345",
        indirect_federal_sharing=1.0,
        legal_entity_state_code="VA",
        legal_entity_county_code="001",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        legal_entity_congressional="01",
        place_of_perfor_state_code="VA",
        place_of_perform_county_co="001",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
        place_of_performance_congr="01",
        _fill_optional=True,
    )
    baker.make(
        "awards.TransactionFABS",
        transaction=asst_trx2,
        cfda_number="12.456",
        action_date="2021-04-01",
        uei="FABSUEI12345",
        indirect_federal_sharing=1.0,
        legal_entity_state_code="VA",
        legal_entity_county_code="001",
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        legal_entity_congressional="01",
        place_of_perfor_state_code="VA",
        place_of_perform_county_co="001",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
        place_of_performance_congr="01",
        _fill_optional=True,
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=cont_trx1,
        naics="123456",
        product_or_service_code="12",
        action_date="2021-07-01",
        awardee_or_recipient_uei="FPDSUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=cont_trx2,
        naics="123456",
        product_or_service_code="12",
        action_date="2021-10-01",
        awardee_or_recipient_uei="FPDSUEI12345",
        _fill_optional=True,
    )

    # Create account data
    federal_account = baker.make(
        "accounts.FederalAccount", parent_toptier_agency=funding_toptier_agency, _fill_optional=True
    )
    tas = baker.make("accounts.TreasuryAppropriationAccount", federal_account=federal_account, _fill_optional=True)
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=asst_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_l,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=asst_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_m,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=cont_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_q,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=cont_award,
        treasury_account=tas,
        disaster_emergency_fund=None,
        _fill_optional=True,
    )

    update_awards()


def _cast_to_string(val: Any) -> str:
    """
    Dictionaries that are converted to strings need to use 'json.dumps()' so we try that first
    and then fallback to the regular 'str()' method.
    """
    try:
        return json.dumps(val)
    except TypeError:
        pass
    return str(val)


def equal_datasets(psql_data: List[Dict[str, Any]], spark_data: List[Dict[str, Any]], custom_schema: str):
    """Helper function to compare the two datasets. Note the column types of ds1 will be used to cast columns in ds2."""
    datasets_match = True

    # Parsing custom_schema to specify
    schema_changes = {}
    schema_type_converters = {
        "INT": int,
        "STRING": _cast_to_string,
    }
    if custom_schema:
        for schema_change in custom_schema.split(","):
            col, new_col_type = schema_change.split()[0].strip(), schema_change.split()[1].strip()
            schema_changes[col] = new_col_type

    # Iterating through the values and finding any differences
    for i, psql_row in enumerate(psql_data):
        for k, psql_val in psql_row.items():
            psql_val_type = type(psql_val)
            spark_val = spark_data[i][k]

            # Casting the postgres values based on the custom schema
            if k.strip() in schema_changes and schema_changes[k].strip() in schema_type_converters:
                psql_val = schema_type_converters[schema_changes[k].strip()](psql_val)

            # Equalize dates - Postgres TIMESTAMPs may include time zones while the Spark TIMESTAMPs may not
            # TODO: Initial implementation by naively stripping off timezone info and comparing core date.
            #  Eventually refactor to normalize timezone-aware datetime objects from psql to UTC, but shifting their
            #  core date and time to UTC before/after stripping timezone info
            if isinstance(psql_val, datetime):
                psql_val = psql_val.replace(tzinfo=None)

            # In the case of a list make sure they are ordered prior to comparison
            if isinstance(psql_val, list):
                psql_val = sorted(psql_val)

            if isinstance(spark_val, list):
                spark_val = sorted(spark_val)

            # Handle JSON formatted strings where the only difference is spacing between keys
            if isinstance(psql_val, str):
                psql_val = psql_val.replace(" ", "")

            if isinstance(spark_val, str):
                spark_val = spark_val.replace(" ", "")

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} " f"left:{psql_val}({psql_val_type}) " f"right:{spark_val}({type(spark_val)})"
                )
    return datasets_match


def _verify_delta_table_loaded(spark: SparkSession, delta_table_name: str, s3_bucket: str, is_aggregate_table=False):
    """Generic function that uses the create_delta_table and load_table_to_delta ommands to create and load the given
    table and assert it was created and loaded as expected
    """
    # make the table and load it
    call_command("create_delta_table", f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}")
    if not is_aggregate_table:
        # aggregate table is loaded during the "create_delta_table" via SELECT AS statement
        call_command("load_table_to_delta", f"--destination-table={delta_table_name}")

    # get the postgres data to compare
    model = TABLE_SPEC[delta_table_name]["model"]
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    dummy_query = model.objects
    if partition_col is not None:
        dummy_query = dummy_query.order_by(partition_col)
    dummy_data = list(dummy_query.all().values())

    # get the spark data to compare
    received_query = f"select * from {delta_table_name}"
    if partition_col is not None:
        received_query = f"{received_query} order by {partition_col}"
    received_data = [row.asDict() for row in spark.sql(received_query).collect()]

    assert equal_datasets(dummy_data, received_data, TABLE_SPEC[delta_table_name]["custom_schema"])


def create_and_load_all_delta_tables(spark: SparkSession, s3_bucket: str):
    non_aggregate_table_specs = {
        key: val
        for key, val in TABLE_SPEC.items()
        if val.get("source_table") is not None and key == val.get("source_table")
    }
    for dest_table in non_aggregate_table_specs:
        call_command("create_delta_table", f"--destination-table={dest_table}", f"--spark-s3-bucket={s3_bucket}")
        call_command("load_table_to_delta", f"--destination-table={dest_table}")
    create_ref_temp_views(spark)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_sam_recipient(spark, s3_unittest_data_bucket):
    baker.make("recipient.DUNS", broker_duns_id="1", _fill_optional=True)
    baker.make("recipient.DUNS", broker_duns_id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "sam_recipient", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_recipient_lookup(spark, s3_unittest_data_bucket):
    baker.make("recipient.RecipientLookup", id="1", _fill_optional=True)
    baker.make("recipient.RecipientLookup", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "recipient_lookup", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_recipient_profile(spark, s3_unittest_data_bucket):
    baker.make("recipient.RecipientProfile", id="1", _fill_optional=True)
    baker.make("recipient.RecipientProfile", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "recipient_profile", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_fabs(spark, s3_unittest_data_bucket):
    # Baker doesn't support autofilling Numeric fields, so we're manually setting them here
    baker.make("awards.TransactionFABS", published_fabs_id="1", indirect_federal_sharing=1.0, _fill_optional=True)
    baker.make("awards.TransactionFABS", published_fabs_id="2", indirect_federal_sharing=1.0, _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_fabs", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_fpds(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="1", _fill_optional=True)
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_fpds", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_normalized(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionNormalized", id="1", _fill_optional=True)
    baker.make("awards.TransactionNormalized", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_normalized", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_search(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)

    _verify_delta_table_loaded(spark, "transaction_search", s3_unittest_data_bucket, is_aggregate_table=True)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_search_testing(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    _verify_delta_table_loaded(spark, "transaction_search_testing", s3_unittest_data_bucket)
