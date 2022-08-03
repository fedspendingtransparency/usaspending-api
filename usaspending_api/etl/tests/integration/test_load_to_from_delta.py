"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import json
import psycopg2
import pytz

from datetime import datetime
from typing import Any, Dict, List, Union

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import fixture, mark

from django.core.management import call_command
from django.db import connection, transaction
from django.db.models import sql

from usaspending_api.common.helpers.spark_helpers import create_ref_temp_views
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC
from usaspending_api.awards.models import TransactionFABS


@fixture
def populate_data_for_transaction_search():
    # Create recipient data for two transactions; the other two will generate ad hoc
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        duns="123456789",
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
        recipient_unique_id="123456789",
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
    baker.make(
        "references.CityCountyStateCode",
        state_alpha="VA",
        county_numeric="001",
        _fill_optional=True,
        county_name="County Name",
    )
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES", _fill_optional=True)
    baker.make("recipient.StateData", code="VA", name="Virginia", fips="51", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="000", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="001", _fill_optional=True)
    baker.make("references.PopCongressionalDistrict", state_code="51", congressional_district="01")
    defc_l = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19", _fill_optional=True)
    defc_m = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19", _fill_optional=True)
    defc_q = baker.make("references.DisasterEmergencyFundCode", code="Q", group_name=None, _fill_optional=True)

    # Create awards and transactions
    asst_award = baker.make(
        "awards.Award",
        type="07",
        period_of_performance_start_date="2021-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2021-01-01",
        total_obligation=100.00,
        total_subsidy_cost=100.00,
        type_description="Direct Loan",
    )
    cont_award = baker.make(
        "awards.Award",
        type="A",
        period_of_performance_start_date="2021-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2021-01-01",
        total_obligation=100.00,
    )

    asst_trx1 = baker.make(
        "awards.TransactionNormalized",
        action_date="2021-01-01",
        award=asst_award,
        is_fpds=False,
        type="07",
        awarding_agency=awarding_agency,
        funding_agency=funding_agency,
        _fill_optional=True,
        last_modified_date="2021-01-01",
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
        last_modified_date="2021-01-01",
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
        last_modified_date="2021-01-01",
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
        last_modified_date="2021-01-01",
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
        ordering_period_end_date="2020-07-01",
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=cont_trx2,
        naics="123456",
        product_or_service_code="12",
        action_date="2021-10-01",
        awardee_or_recipient_uei="FPDSUEI12345",
        _fill_optional=True,
        ordering_period_end_date="2020-07-01",
    )

    # Create account data
    federal_account = baker.make(
        "accounts.FederalAccount", parent_toptier_agency=funding_toptier_agency, _fill_optional=True
    )
    tas = baker.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=federal_account,
        allocation_transfer_agency_id=None,
        _fill_optional=True,
    )
    dabs = baker.make("submissions.DABSSubmissionWindowSchedule", submission_reveal_date="2020-05-01")
    sa = baker.make("submissions.SubmissionAttributes", reporting_period_start="2020-04-02", submission_window=dabs)

    baker.make(
        "awards.FinancialAccountsByAwards",
        award=asst_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_l,
        _fill_optional=True,
        submission=sa,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=asst_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_m,
        _fill_optional=True,
        submission=sa,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=cont_award,
        treasury_account=tas,
        disaster_emergency_fund=defc_q,
        _fill_optional=True,
        submission=sa,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=cont_award,
        treasury_account=tas,
        disaster_emergency_fund=None,
        _fill_optional=True,
        submission=sa,
    )

    update_awards()


def _handle_string_cast(val: str) -> Union[str, dict, list]:
    """
    JSON nested element columns are represented as JSON formatted strings in the Spark data, but nested elements in the
    Postgres data. Both columns will have a "custom_schema" defined for "<FIELD_TYPE> STRING". To handle this comparison
    the casting of values for "custom_schema" of STRING will attempt to convert a string to a nested element in the case
    of a nested element and fallback to a simple string cast.
    """
    if isinstance(val, list):
        try:
            casted = [json.loads(element) if isinstance(element, str) else element for element in val]
        except (TypeError, json.decoder.JSONDecodeError):
            casted = [str(element) for element in val]
    elif isinstance(val, dict):
        try:
            casted = {k: json.loads(element) if isinstance(element, str) else element for k, element in val.items()}
        except (TypeError, json.decoder.JSONDecodeError):
            casted = {k: str(element) for k, element in val.items()}
    else:
        try:
            casted = json.loads(val)
        except (TypeError, json.decoder.JSONDecodeError):
            casted = str(val)
    return casted


def sorted_deep(d):
    def make_tuple(v):
        if isinstance(v, list):
            return (*sorted_deep(v),)
        if isinstance(v, dict):
            return (*sorted_deep(list(v.items())),)
        return (v,)

    if isinstance(d, list):
        return sorted(map(sorted_deep, d), key=make_tuple)
    if isinstance(d, dict):
        return {k: sorted_deep(d[k]) for k in sorted(d)}
    return d


def equal_datasets(psql_data: List[Dict[str, Any]], spark_data: List[Dict[str, Any]], custom_schema: str):
    """Helper function to compare the two datasets. Note the column types of ds1 will be used to cast columns in ds2."""
    datasets_match = True

    # Parsing custom_schema to specify
    schema_changes = {}
    schema_type_converters = {"INT": int, "STRING": _handle_string_cast, "ARRAY<STRING>": _handle_string_cast}
    if custom_schema:
        for schema_change in custom_schema.split(", "):
            col, new_col_type = schema_change.split()[0].strip(), schema_change.split()[1].strip()
            schema_changes[col] = new_col_type

    # Iterating through the values and finding any differences
    for i, psql_row in enumerate(psql_data):
        for k, psql_val in psql_row.items():
            spark_val = spark_data[i][k]
            # Casting values based on the custom schema
            if (
                k.strip() in schema_changes
                and schema_changes[k].strip() in schema_type_converters
                and psql_val is not None
            ):
                spark_val = schema_type_converters[schema_changes[k].strip()](spark_val)
                psql_val = schema_type_converters[schema_changes[k].strip()](psql_val)

            # Equalize dates
            # - Postgres TIMESTAMPs may include time zones
            # - while Spark TIMESTAMPs will not
            #   - they are aligned to the Spark SQL session Time Zone (by way
            #     of spark.sql.session.timeZone conf setting) at the time they are read)
            #   - From: https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#timestamps-and-time-zones
            #   - "When writing timestamp values out to non-text data sources like Parquet, the values are just
            #      instants (like timestamp in UTC) that have no time zone information."
            if isinstance(psql_val, datetime):
                # Align to what Spark:
                #   1. Align the date to UTC, to shift the date the amount of time from the offset to UTC
                #   2. Then strip off the UTC time zone info
                utc_tz = pytz.timezone("UTC")
                psql_val_utc = psql_val.astimezone(utc_tz)
                psql_val = psql_val_utc.replace(tzinfo=None)

            # Make sure Postgres data is sorted in the case of a list since the Spark list data is sorted in ASC order
            if isinstance(psql_val, list):
                psql_val = sorted_deep(psql_val)
                if isinstance(spark_val, str):
                    spark_val = [json.loads(idx.replace("'", '"')) for idx in [spark_val]][0]
                spark_val = sorted_deep(spark_val)

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} "
                    f"left(psql):{psql_val} ({type(psql_val)}) "
                    f"right(spark):{spark_val} ({type(spark_val)})"
                )
    return datasets_match


def _verify_delta_table_loaded_to_delta(
    spark: SparkSession,
    delta_table_name: str,
    s3_bucket: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command="load_table_to_delta",
):
    """Generic function that uses the create_delta_table and load_table_to_delta commands to create and load the given
    table and assert it was created and loaded as expected
    """

    cmd_args = [f"--destination-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-db={alt_db}"]
    expected_table_name = delta_table_name
    if alt_name:
        cmd_args += [f"--alt-name={alt_name}"]
        expected_table_name = alt_name

    # make the table and load it
    call_command("create_delta_table", f"--spark-s3-bucket={s3_bucket}", *cmd_args)
    call_command(load_command, *cmd_args)

    # get the postgres data to compare
    model = TABLE_SPEC[delta_table_name]["model"]
    partition_col = TABLE_SPEC[delta_table_name].get("partition_column")
    dummy_query = model.objects
    if partition_col is not None:
        dummy_query = dummy_query.order_by(partition_col)
    dummy_data = list(dummy_query.all().values())

    # get the spark data to compare
    # NOTE: The ``use <db>`` from table create/load is still in effect for this verification. So no need to call again
    received_query = f"SELECT * from {expected_table_name}"
    if partition_col is not None:
        received_query = f"{received_query} ORDER BY {partition_col}"
    received_data = [row.asDict() for row in spark.sql(received_query).collect()]

    assert equal_datasets(dummy_data, received_data, TABLE_SPEC[delta_table_name]["custom_schema"])


def _verify_delta_table_loaded_from_delta(
    spark: SparkSession,
    delta_table_name: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command="load_table_from_delta",
):
    """Generic function that uses the load_table_from_delta commands to load the given table and assert it was
    downloaded as expected
    """
    cmd_args = [f"--delta-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-delta-db={alt_db}"]
    expected_table_name = delta_table_name
    if alt_name:
        cmd_args += [f"--alt-delta-name={alt_name}"]
        expected_table_name = alt_name

    # table already made, let's load it
    call_command(load_command, *cmd_args)

    # get the postgres data to compare
    source_table = TABLE_SPEC[delta_table_name]["source_table"] or TABLE_SPEC[delta_table_name]["swap_table"]
    temp_schema = "temp"
    if source_table:
        tmp_table_name = f"{temp_schema}.{source_table}_temp"
    else:
        tmp_table_name = f"{temp_schema}.{expected_table_name}_temp"
    postgres_query = f"SELECT * FROM {tmp_table_name}"
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    if partition_col is not None:
        postgres_query = f"{postgres_query} ORDER BY {partition_col}"
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(postgres_query)
            postgres_data = dictfetchall(cursor)

    # get the spark data to compare
    delta_query = f"SELECT * FROM {expected_table_name}"
    if partition_col is not None:
        delta_query = f"{delta_query} ORDER BY {partition_col}"
    delta_data = [row.asDict() for row in spark.sql(delta_query).collect()]

    assert equal_datasets(postgres_data, delta_data, TABLE_SPEC[delta_table_name]["custom_schema"])


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
def test_load_table_to_from_delta_for_sam_recipient_and_reload(spark, s3_unittest_data_bucket):
    baker.make("recipient.DUNS", broker_duns_id="1", _fill_optional=True)
    baker.make("recipient.DUNS", broker_duns_id="2", _fill_optional=True)
    _verify_delta_table_loaded_to_delta(spark, "sam_recipient", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "sam_recipient")

    # Getting count of the first load
    expected_exported_table = "duns_temp"
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            # get a list of tables
            cursor.execute(f"SELECT COUNT(*) FROM {expected_exported_table}")
            expected_exported_table_count = dictfetchall(cursor)[0]["count"]

    # Rerunning again to see it working as intended
    _verify_delta_table_loaded_from_delta(spark, "sam_recipient")

    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            # get a list of tables and ensure no other new tables got made
            cursor.execute("SELECT * FROM pg_catalog.pg_tables;")
            postgres_data = [
                d["tablename"] for d in dictfetchall(cursor) if d["tablename"].startswith(expected_exported_table)
            ]
            assert len(postgres_data) == 1

            # make sure the new table has been truncated and reloaded
            cursor.execute(f"SELECT COUNT(*) FROM {expected_exported_table}")
            expected_new_exported_table_count = dictfetchall(cursor)[0]["count"]
            assert expected_exported_table_count == expected_new_exported_table_count


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_recipient_lookup(spark, s3_unittest_data_bucket):
    baker.make("recipient.RecipientLookup", id="1", _fill_optional=True)
    baker.make("recipient.RecipientLookup", id="2", _fill_optional=True)
    _verify_delta_table_loaded_to_delta(spark, "recipient_lookup", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "recipient_lookup")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_fabs(spark, s3_unittest_data_bucket):
    # Baker doesn't support autofilling Numeric fields, so we're manually setting them here
    baker.make("awards.TransactionFABS", published_fabs_id="1", indirect_federal_sharing=1.0, _fill_optional=True)
    baker.make("awards.TransactionFABS", published_fabs_id="2", indirect_federal_sharing=1.0, _fill_optional=True)
    _verify_delta_table_loaded_to_delta(spark, "transaction_fabs", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "transaction_fabs")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_fabs_timezone_aware(spark, s3_unittest_data_bucket):
    """Test that timestamps are not inadvertently shifted due to loss of timezone during reads and writes.

    The big takeaways from this are:
    BLUF: Keep Django oriented to UTC (we're doing this) and keep the same timezone set in spark sessions where you
    are reading and writing data (we're doing this)

    1. Postgres stores its fields with data type ``timestamp with time zone`` under the hood in UTC, and it's up to
       the connection (and the ``time zone`` configuration parameter on that connection) to determine how it wants to
       LOOK AT that timezone-aware data (from what time zone perspective).
    2. All the django reads/writes are done via a UTC connection, because we use USE_TZ=True and TIME_ZONE="UTC" in
       settings.py
    3. But even if it weren't, it doesn't matter, since the data is stored in UTC in postgres regardless of what the
       settings where
    4. Where that Django conn time zone does matter, is if we naively strip off the timezone from a timezone-aware
       datetime value that was read under non-UTC settings, and assume its value is UTC-aligned. But we're luckily
       protected from any naivete like this by those settings.py settings.
    5. Spark will store Parquet data as an instant, without any timezone information. It's up to the session
       in which the data is being read to infer any timezone part on that instant. This is governed by the
       spark.sql.session.timeZone setting. If you read and write data with the same session timeZone, all is good (
       seeing it from the same perspective). But if you were to write it with one time zone and read it with another,
       it would be inadvertently adding/removing hours from the stored time instant (as it was written).
    """
    # Add another record with explict timezone on it
    tz_hst = pytz.timezone("HST")  # Hawaii–Aleutian Standard Time = UTC-10:00
    tz_utc = pytz.timezone("UTC")
    dt_naive = datetime(2022, 6, 11, 11, 11, 11)
    dt_with_tz = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_hst)
    dt_with_utc = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_utc)
    # Because if they are reflecting the same day+hour, then the HST (further east/back) TZ with a -10
    # offset is actually 10 hours ahead of the stated day+hour when looked at in the UTC timezone
    assert dt_with_utc.timestamp() < dt_with_tz.timestamp()

    # Prepare a model object without saving it, but do save the related fields
    # - https://model-bakery.readthedocs.io/en/latest/basic_usage.html#non-persistent-objects
    # Do this so we can save the TransactionFABS record without interference from the Django DB connections
    # Session settings (like sesssion-set time zone)
    fabs_with_tz = baker.prepare(
        "awards.TransactionFABS",
        _save_related=True,
        published_fabs_id="3",
        indirect_federal_sharing=1.0,
        modified_at=dt_with_tz,
        _fill_optional=True,
    )  # type: TransactionFABS

    def _get_sql_insert_from_model(model):
        values = model._meta.local_fields
        q = sql.InsertQuery(model)
        q.insert_values(values, [model])
        compiler = q.get_compiler("default")
        setattr(compiler, "return_id", False)
        stmts = compiler.as_sql()
        stmt = [
            stmt % tuple(f"'{param}'" if type(param) in [str, datetime] else param for param in params)
            for stmt, params in stmts
        ]
        return stmt[0]

    # Now save it to the test DB using a new connection, that establishes its own time zone during it session
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            fabs_insert_sql = _get_sql_insert_from_model(fabs_with_tz)
            cursor.execute(fabs_insert_sql)
            assert cursor.rowcount == 1
            new_psycopg2_conn.commit()

    # See how things look from Django's perspective
    with transaction.atomic():
        # Fetch the DB object in a new transaction
        fabs_with_tz = TransactionFABS.objects.filter(published_fabs_id="3").first()
        assert fabs_with_tz is not None

        # Check that all dates are as expected
        model_datetime = fabs_with_tz.modified_at  # type: datetime
        assert model_datetime.tzinfo is not None

        # NOTE: this is because of our Django settings. Upon saving timezone-aware data, it shifts it to UTC
        # Specifically, in settings.py, you will find
        #   TIME_ZONE = "UTC"
        #   USE_TZ = True
        # And this value STICKS in the long-lived django ``connection`` object. So long as that is used (with the ORM
        # or with raw SQL), it will apply those time zone settings
        assert model_datetime.tzname() != "HST"
        assert model_datetime.tzname() == "UTC"
        assert model_datetime.hour == 21  # shifted +10 to counteract the UTC offset by django upon saving it
        assert model_datetime.utctimetuple().tm_hour == 21  # already shifted to UTC, so this just matches .hour (== 21)
        assert dt_naive.utctimetuple().tm_hour == dt_naive.hour  # naive, so stays the same
        assert dt_with_utc.utctimetuple().tm_hour == dt_with_utc.hour  # already UTC, so stays the same

        # Confirm also that this is the case in the DB (i.e. it was at write-time that UTC was set, not read-time
        with connection.cursor() as cursor:
            cursor.execute("select modified_at from transaction_fabs where published_fabs_id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            assert dt_from_db.tzname() == "UTC"

        # Confirm whether the Test DB created was forced to UTC timezone based on settings.py
        # (Spoiler, yes it does)
        with connection.cursor() as cursor:
            cursor.execute("show time zone")
            db_tz = [row[0] for row in cursor.fetchall()][0]
            assert db_tz is not None
            assert db_tz == "UTC"

    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            cursor.execute("select modified_at from transaction_fabs where published_fabs_id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            # Can't use traditional time zone names with tzname() since pyscopg2 uses its own time zone infos.
            # Use psycopg2 tzinfo name and then compare their delta
            assert dt_from_db.tzname() == "-10"
            assert dt_from_db.utcoffset().total_seconds() == -36000.0

    # Now with that DB data committed, and with the DB set to HST TIME ZONE, do a Spark read
    try:
        # Hijack the spark time zone setting for the purposes of this test to make it NOT UTC
        # This should not matter so long as the data is WRITTEN (e.g. to parquet)
        # and READ (e.g. from a Delta Table over that parquet into a DataFrame) under the SAME timezone
        original_spark_tz = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        _verify_delta_table_loaded_to_delta(spark, "transaction_fabs", s3_unittest_data_bucket)
        _verify_delta_table_loaded_from_delta(spark, "transaction_fabs")
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_spark_tz)


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_fpds(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="1", _fill_optional=True)
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="2", _fill_optional=True)
    _verify_delta_table_loaded_to_delta(spark, "transaction_fpds", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "transaction_fpds")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_normalized(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionNormalized", id="1", _fill_optional=True)
    baker.make("awards.TransactionNormalized", id="2", _fill_optional=True)
    _verify_delta_table_loaded_to_delta(spark, "transaction_normalized", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "transaction_normalized")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)
    _verify_delta_table_loaded_to_delta(
        spark, "transaction_search", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    _verify_delta_table_loaded_from_delta(spark, "transaction_search")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search_testing(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    _verify_delta_table_loaded_to_delta(spark, "transaction_search_testing", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "transaction_search_testing")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_normalized_alt_db_and_name(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionNormalized", id="1", _fill_optional=True)
    baker.make("awards.TransactionNormalized", id="2", _fill_optional=True)
    _verify_delta_table_loaded_to_delta(
        spark,
        "transaction_normalized",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_normalized_alt_name",
    )
    _verify_delta_table_loaded_from_delta(
        spark,
        "transaction_normalized",
        alt_db="my_alt_db",
        alt_name="transaction_normalized_alt_name",
    )


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search_alt_db_and_name(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)
    _verify_delta_table_loaded_to_delta(
        spark,
        "transaction_search",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_search_alt_name",
        load_command="load_query_to_delta",
    )
    _verify_delta_table_loaded_from_delta(
        spark,
        "transaction_search",
        alt_db="my_alt_db",
        alt_name="transaction_search_alt_name",
    )


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_award_search(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)
    _verify_delta_table_loaded_to_delta(
        spark, "award_search", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    _verify_delta_table_loaded_from_delta(spark, "award_search")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_award_search_testing(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    _verify_delta_table_loaded_to_delta(spark, "award_search_testing", s3_unittest_data_bucket)
    _verify_delta_table_loaded_from_delta(spark, "award_search_testing")


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_award_search_alt_db_and_name(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)
    _verify_delta_table_loaded_to_delta(
        spark,
        "award_search",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="award_search_alt_name",
        load_command="load_query_to_delta",
    )
    _verify_delta_table_loaded_from_delta(
        spark,
        "award_search",
        alt_db="my_alt_db",
        alt_name="award_search_alt_name",
    )


@mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_recipient_profile(
    spark, s3_unittest_data_bucket, populate_data_for_transaction_search
):
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket)
    _verify_delta_table_loaded_to_delta(
        spark, "recipient_profile", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    _verify_delta_table_loaded_from_delta(spark, "recipient_profile")
