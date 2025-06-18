import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Generator

import boto3
import pytest
from django.core.management import call_command
from django.db import connections
from model_bakery import baker
from psycopg2.extensions import AsIs
from usaspending_api import settings
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    is_spark_context_stopped,
    stop_spark_context,
)
from usaspending_api.common.spark.configs import LOCAL_BASIC_EXTRA_CONF
from usaspending_api.config import CONFIG
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.management.commands.create_delta_table import LOAD_QUERY_TABLE_SPEC, LOAD_TABLE_TABLE_SPEC

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ==== Spark Automated Integration Test Fixtures ==== #

# How to determine a working dependency set:
# 1. What platform are you using? local dev with pip-installed PySpark? EMR 6.x or 5.x? Databricks Runtime?
# 2. From there determine what versions of Spark + Hadoop are supported on that platform. If going cross-platform,
#    try to pick a combo that's supported on both
# 3. Is there a hadoop-aws version matching the platform's Hadoop version used? Because we need to have Spark writing
#    to S3, we are beholden to the AWS-provided JARs that implement the S3AFileSystem, which are part of the
#    hadoop-aws JAR.
# 4. Going from the platform-hadoop version, find the same version of hadoop-aws up in
#    https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/
#    and look to see what version its dependent JARs are at that your code requires are runtime. If seeing errors or are
#    uncertain of compatibility, see what working version-sets are aligned to an Amazon EMR release here:
#    https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html

DELTA_LAKE_UNITTEST_SCHEMA_NAME = "unittest"


@pytest.fixture(scope="session")
def s3_unittest_data_bucket_setup_and_teardown(worker_id: str) -> str:
    """Create a test bucket so the tests can use it

    Args:
        worker_id: ID of worker if this session is one of multiple in a parallel pytest-xdist run, used as a suffix
        to the unit test S3 bucket if provided.

    Returns:
        unittest_data_bucket: Bucket name of the unit test S3 bucket created for this pytest session
    """
    worker_prefix = "" if (not worker_id or worker_id == "master") else worker_id + "-"
    unittest_data_bucket = "unittest-data-{}".format(worker_prefix + str(uuid.uuid4()))

    logging.warning(
        f"Attempting to create unit test data bucket {unittest_data_bucket } "
        f"at: http://{CONFIG.AWS_S3_ENDPOINT} using CONFIG.AWS_ACCESS_KEY and CONFIG.AWS_SECRET_KEY"
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    from botocore.errorfactory import ClientError

    try:
        s3_client.create_bucket(Bucket=unittest_data_bucket)
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            # Simplest way to ensure the bucket is created is to swallow the exception saying it already exists
            logging.warning("Unit Test Data Bucket not created; already exists.")
            pass
        else:
            raise e

    logging.info(
        f"Unit Test Data Bucket '{unittest_data_bucket}' created (or found to exist) at S3 endpoint "
        f"'{unittest_data_bucket}'. Current Buckets:"
    )
    [logging.info(f"  {b['Name']}") for b in s3_client.list_buckets()["Buckets"]]

    yield unittest_data_bucket

    # Cleanup by removing all objects in the bucket by key, and then the bucket itsefl after the test session
    response = s3_client.list_objects_v2(Bucket=unittest_data_bucket)
    if "Contents" in response:
        for object in response["Contents"]:
            s3_client.delete_object(Bucket=unittest_data_bucket, Key=object["Key"])
    s3_client.delete_bucket(Bucket=unittest_data_bucket)


@pytest.fixture(scope="function")
def s3_unittest_data_bucket(s3_unittest_data_bucket_setup_and_teardown):
    """Use the S3 unit test data bucket created for the test session, and cleanup any contents created in it after
    each test
    """
    unittest_data_bucket = s3_unittest_data_bucket_setup_and_teardown
    yield unittest_data_bucket

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    # Cleanup any contents added to the bucket for this test
    response = s3_client.list_objects_v2(Bucket=unittest_data_bucket)
    if "Contents" in response:
        for object in response["Contents"]:
            s3_client.delete_object(Bucket=unittest_data_bucket, Key=object["Key"])
    # NOTE: Leave the bucket itself there for other tests in this session. It will get cleaned up at the end of the
    # test session by the dependent fixture


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> Generator["SparkSession", None, None]:
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes

    This fixture will create ONE single SparkContext to be shared by ALL unit tests (and therefore must be populated
    with universally compatible config and with the superset of all JAR dependencies our test code might need.
    """
    if not is_spark_context_stopped():
        raise Exception(
            "Error: Test session cannot create a SparkSession because one already exists at the time this "
            "test-session-scoped fixture is being evaluated."
        )

    # Storing spark warehouse and hive metastore_db in a tmpdir so it does not leave cruft behind from test session runs
    # So as not to have interfering schemas and tables in the metastore_db from individual test run to run,
    # another test-scoped fixture should be created, pulling this in, and blowing away all schemas and tables as part
    # of each run
    spark_sql_warehouse_dir = str(tmp_path_factory.mktemp(basename="spark-warehouse", numbered=False))
    extra_conf = {
        **LOCAL_BASIC_EXTRA_CONF,
        "spark.sql.warehouse.dir": spark_sql_warehouse_dir,
        "spark.hadoop.javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={spark_sql_warehouse_dir}/metastore_db;create=true",
    }
    spark = configure_spark_session(
        app_name="Unit Test Session",
        log_level=logging.INFO,
        log_spark_config_vals=True,
        enable_hive_support=True,
        **extra_conf,
    )  # type: SparkSession

    yield spark

    stop_spark_context()


@pytest.fixture
def hive_unittest_metastore_db(spark: "SparkSession"):
    """A fixture that WIPES all of the schemas (aka databases) and tables in each schema from the hive metastore_db
    at the end of each test run, so that the metastore is fresh.

    NOTE: This relies on setup in the session-scoped ``spark`` fixture:
      - That fixture must enableHiveSupport() when creating the SparkSession
      - That fixture needs to set the filesystem location of the hive metastore_db (Derby DB) folder in a tmp dir
        - (so that it doesn't interfere or leave cruft behind)
      - That fixture needs to set the Spark SQL Warehouse dir in a tmp dir
        - (so that it doesn't interfere or leave cruft behind)

    WARNING: If the spark test fixture is not setup to initialize the hive metastore_db in this way for the
    SparkSession used by tests, then this fixture may inadvertently wipe all hive schemas and tables in you dev env
    """
    metastore_db_path = None
    metastore_db_url = spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionURL")
    if metastore_db_url:
        metastore_db_path = metastore_db_url.split("=")[1].split(";")[0]

    yield metastore_db_path

    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]

    # Cascade will remove the tables and functions in each SCHEMA *other than* the default (cannot drop that one)
    for s in schemas_in_metastore:
        if s == "default":
            continue
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")

    # Handle default schema specially
    spark.sql("USE DEFAULT")
    tables_in_default_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    for t in tables_in_default_schema:
        spark.sql(f"DROP TABLE IF EXISTS {t['tableName']}")


@pytest.fixture
def delta_lake_unittest_schema(spark: "SparkSession", hive_unittest_metastore_db):
    """Specify which Delta 'SCHEMA' to use (NOTE: 'SCHEMA' and 'DATABASE' are interchangeable in Delta Spark SQL),
    and cleanup any objects created in the schema after the test run."""

    # Force default usage of the unittest schema in this SparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")
    spark.sql(f"USE {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")

    # Yield the name of the db that test delta lake tables and records should be put in.
    yield DELTA_LAKE_UNITTEST_SCHEMA_NAME

    # The dependent hive_unittest_metastore_db fixture will take care of cleaning up this schema post-test


@pytest.fixture
def populate_broker_data(broker_server_dblink_setup):
    """Fixture to INSERT data stubbed out in JSON files into the Broker database, to be used by tests that consume
    Broker data.

    Data will be committed to the Broker DB during the test, then removed in the "tear down" phase of this fixture by
    table TRUNCATE statements.

    Args:
        broker_server_dblink_setup: Fixture this one depends on, to ensure that a DB Link connection exists between
            USAspending test DB and broker test DB
    """
    broker_data = {
        "sam_recipient": json.loads(Path("usaspending_api/recipient/tests/data/broker_sam_recipient.json").read_text()),
        "subaward": json.loads(Path("usaspending_api/awards/tests/data/subaward.json").read_text()),
        "cd_state_grouped": json.loads(
            Path("usaspending_api/transactions/tests/data/cd_state_grouped.json").read_text()
        ),
        "zips": json.loads(Path("usaspending_api/transactions/tests/data/zips.json").read_text()),
        "cd_zips_grouped": json.loads(Path("usaspending_api/transactions/tests/data/cd_zips_grouped.json").read_text()),
        "cd_city_grouped": json.loads(Path("usaspending_api/transactions/tests/data/cd_city_grouped.json").read_text()),
        "cd_county_grouped": json.loads(
            Path("usaspending_api/transactions/tests/data/cd_county_grouped.json").read_text()
        ),
    }
    insert_statement = "INSERT INTO %(table_name)s (%(columns)s) VALUES %(values)s"
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        for table_name, rows in broker_data.items():
            # An assumption is made that each set of rows have the same columns in the same order
            columns = list(rows[0])
            values = [str(tuple(r.values())).replace("None", "null") for r in rows]
            sql_string = cursor.mogrify(
                insert_statement,
                {"table_name": AsIs(table_name), "columns": AsIs(",".join(columns)), "values": AsIs(",".join(values))},
            )
            cursor.execute(sql_string)
    yield
    # Cleanup test data for each Broker test table
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        for table in broker_data:
            cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")


def _build_usas_data_for_spark():
    """Create Django model data to be saved to Postgres, which will then get ingested into Delta Lake"""
    # Create recipient data for two transactions; the other two will generate ad hoc
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        duns="FABSDUNS12345",
        legal_business_name="FABS TEST RECIPIENT",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientLookup",
        uei="PARENTUEI12345",
        duns="PARENTDUNS12345",
        legal_business_name="PARENT RECIPIENT 12345",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        uei="FPDSUEI12345",
        duns="FPDSDUNS12345",
        legal_business_name="FPDS RECIPIENT 12345",
        parent_uei="PARENTUEI12345",
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        uei="FABSUEI12345",
        recipient_level="C",
        recipient_name="FABS TEST RECIPIENT",
        recipient_unique_id="FABSDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["PARENTUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        uei="PARENTUEI12345",
        recipient_level="P",
        recipient_name="PARENT RECIPIENT 12345",
        recipient_unique_id="PARENTDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["FABSUEI12345", "FPDSUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        uei="FPDSUEI12345",
        recipient_level="C",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_unique_id="FPDSDUNS12345",
        parent_uei="PARENTUEI12345",
        recipient_affiliations=["PARENTUEI12345"],
        _fill_optional=True,
    )
    baker.make(
        "recipient.DUNS",
        broker_duns_id="1",
        uei="FABSUEI12345",
        ultimate_parent_uei="PARENTUEI12345",
        ultimate_parent_unique_ide="PARENTDUNS12345",
        awardee_or_recipient_uniqu="FABSDUNS12345",
        ultimate_parent_legal_enti="PARENT RECIPIENT 12345",
        legal_business_name="FABS TEST RECIPIENT",
        _fill_optional=True,
    )

    # Create agency data
    funding_toptier_agency = baker.make(
        "references.ToptierAgency", name="TEST AGENCY 1", abbreviation="TA1", _fill_optional=True
    )
    funding_subtier_agency = baker.make(
        "references.SubtierAgency", name="TEST SUBTIER 1", abbreviation="SA1", _fill_optional=True
    )
    funding_agency = baker.make(
        "references.Agency",
        toptier_agency=funding_toptier_agency,
        subtier_agency=funding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    toptier = baker.make("references.ToptierAgency", name="toptier", abbreviation="tt", _fill_optional=True)
    subtier = baker.make("references.SubtierAgency", name="subtier", abbreviation="st", _fill_optional=True)
    agency = baker.make(
        "references.Agency",
        toptier_agency=toptier,
        subtier_agency=subtier,
        toptier_flag=True,
        id=32,
        _fill_optional=True,
    )

    awarding_toptier_agency = baker.make(
        "references.ToptierAgency", name="TEST AGENCY 2", abbreviation="TA2", _fill_optional=True
    )
    awarding_subtier_agency = baker.make(
        "references.SubtierAgency", name="TEST SUBTIER 2", abbreviation="SA2", subtier_code="789", _fill_optional=True
    )
    awarding_agency = baker.make(
        "references.Agency",
        toptier_agency=awarding_toptier_agency,
        subtier_agency=awarding_subtier_agency,
        toptier_flag=True,
        _fill_optional=True,
    )

    # Create reference data
    baker.make("references.NAICS", code="123456", _fill_optional=True)
    psc = baker.make("references.PSC", code="12", _fill_optional=True)
    cfda = baker.make("references.Cfda", program_number="12.456", _fill_optional=True)
    baker.make(
        "references.CityCountyStateCode",
        state_alpha="VA",
        county_numeric="001",
        county_name="County Name",
        _fill_optional=True,
    )
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES", _fill_optional=True)
    baker.make("recipient.StateData", code="VA", name="Virginia", fips="51", _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="000", latest_population=1, _fill_optional=True)
    baker.make("references.PopCounty", state_code="51", county_number="001", latest_population=1, _fill_optional=True)
    baker.make("references.PopCongressionalDistrict", state_code="51", latest_population=1, congressional_district="01")
    defc_l = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19", _fill_optional=True)
    defc_m = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19", _fill_optional=True)
    defc_q = baker.make("references.DisasterEmergencyFundCode", code="Q", group_name=None, _fill_optional=True)
    rpa_1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code="0001",
        program_activity_name="OFFICE OF THE SECRETARY",
    )
    rpa_2 = baker.make(
        "references.RefProgramActivity",
        id=2,
        program_activity_code="0002",
        program_activity_name="OPERATIONS AND MAINTENANCE",
    )
    rpa_3 = baker.make(
        "references.RefProgramActivity",
        id=3,
        program_activity_code="0003",
        program_activity_name="TRAINING AND RECRUITING",
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
    # Create awards and transactions
    asst_award = baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=2,
        earliest_transaction_search_id=1,
        latest_transaction_search_id=2,
        type_raw="07",
        type="07",
        category="loans",
        generated_unique_award_id="UNIQUE AWARD KEY B",
        generated_unique_award_id_legacy="ASST_NON_FAIN_789",
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-01-01",
        certified_date="2020-04-01",
        update_date="2020-01-01",
        action_date="2020-04-01",
        fiscal_year=2020,
        award_amount=0.00,
        total_outlays=2.0,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_loan_value=0.00,
        total_obl_bin="<1M",
        type_description="Direct Loan",
        display_award_id="FAIN",
        fain="FAIN",
        uri="URI",
        piid=None,
        is_fpds=False,
        subaward_count=0,
        transaction_unique_id=2,
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_code=awarding_toptier_agency.toptier_code,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_code=funding_toptier_agency.toptier_code,
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_code=awarding_subtier_agency.subtier_code,
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_code=funding_subtier_agency.subtier_code,
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        funding_toptier_agency_id=funding_agency.id,
        funding_subtier_agency_id=funding_agency.id,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        cfda_number="12.456",
        cfdas=[json.dumps({"cfda_number": "12.456", "cfda_program_title": None})],
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        raw_recipient_name="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        recipient_location_state_code="VA",
        recipient_location_state_name="Virginia",
        recipient_location_state_fips=51,
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_name="Virginia",
        pop_state_fips=51,
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        disaster_emergency_fund_codes=["L", "M"],
        total_covid_outlay=2.0,
        total_covid_obligation=2.0,
        spending_by_defc=[
            {"defc": "L", "outlay": 1.0, "obligation": 1.0},
            {"defc": "M", "outlay": 1.0, "obligation": 1.0},
        ],
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        recipient_location_county_fips="51001",
        pop_county_fips="51001",
        generated_pragmatic_obligation=0.00,
        program_activities=[
            {"name": "OFFICE OF THE SECRETARY", "code": "0001"},
            {"name": "OPERATIONS AND MAINTENANCE", "code": "0002"},
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
    )
    asst_award2 = baker.make(
        "search.AwardSearch",
        award_id=4,
        latest_transaction_id=5,
        earliest_transaction_search_id=5,
        latest_transaction_search_id=5,
        type_raw="02",
        type="02",
        category="grant",
        generated_unique_award_id="UNIQUE AWARD KEY D",
        generated_unique_award_id_legacy="ASST_AGG_URI123_789",
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-04-01",
        certified_date="2020-04-01",
        update_date="2020-01-01",
        action_date="2020-04-01",
        fiscal_year=2020,
        award_amount=0.00,
        total_outlays=None,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_loan_value=0.00,
        total_obl_bin="<1M",
        type_description="BLOCK GRANT (A)",
        display_award_id="FAIN123",
        fain="FAIN123",
        uri="URI123",
        piid=None,
        is_fpds=False,
        subaward_count=0,
        transaction_unique_id=5,
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_code=awarding_toptier_agency.toptier_code,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_code=funding_toptier_agency.toptier_code,
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_code=awarding_subtier_agency.subtier_code,
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_code=funding_subtier_agency.subtier_code,
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        funding_toptier_agency_id=funding_agency.id,
        funding_subtier_agency_id=funding_agency.id,
        treasury_account_identifiers=None,
        cfda_number="12.456",
        cfdas=[json.dumps({"cfda_number": "12.456", "cfda_program_title": None})],
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        raw_recipient_name="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        recipient_location_state_code="VA",
        recipient_location_state_name="Virginia",
        recipient_location_state_fips=51,
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_name="Virginia",
        pop_state_fips=51,
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        tas_paths=None,
        tas_components=None,
        disaster_emergency_fund_codes=None,
        total_covid_outlay=None,
        total_covid_obligation=None,
        spending_by_defc=None,
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        recipient_location_county_fips="51001",
        pop_county_fips="51001",
        generated_pragmatic_obligation=0.00,
        program_activities=None,
        federal_accounts=None,
    )
    cont_award = baker.make(
        "search.AwardSearch",
        award_id=2,
        type_raw="A",
        type="A",
        category="contract",
        generated_unique_award_id="UNIQUE AWARD KEY C",
        generated_unique_award_id_legacy=None,
        latest_transaction_id=4,
        earliest_transaction_search_id=3,
        latest_transaction_search_id=4,
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-07-01",
        certified_date="2020-10-01",
        update_date="2020-01-01",
        action_date="2020-10-01",
        award_amount=0.00,
        total_outlays=3.0,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_obl_bin="<1M",
        display_award_id="PIID",
        piid="PIID",
        fain=None,
        uri=None,
        is_fpds=True,
        subaward_count=0,
        transaction_unique_id=2,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        raw_recipient_name="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_code=awarding_toptier_agency.toptier_code,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_code=funding_toptier_agency.toptier_code,
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_code=awarding_subtier_agency.subtier_code,
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_code=funding_subtier_agency.subtier_code,
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        funding_toptier_agency_id=funding_agency.id,
        funding_subtier_agency_id=funding_agency.id,
        recipient_location_state_code="VA",
        recipient_location_state_name="Virginia",
        recipient_location_state_fips=51,
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code_current=None,
        cfdas=None,
        pop_state_code="VA",
        pop_state_name="Virginia",
        pop_state_fips=51,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        disaster_emergency_fund_codes=["Q"],
        spending_by_defc=[{"defc": "Q", "outlay": 1.00, "obligation": 1.00}],
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        ordering_period_end_date="2020-07-01",
        naics_code="123456",
        product_or_service_code="12",
        product_or_service_description=psc.description,
        recipient_location_county_fips=None,
        pop_county_fips=None,
        generated_pragmatic_obligation=0.00,
        program_activities=[{"name": "TRAINING AND RECRUITING", "code": "0003"}],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
    )
    cont_award2 = baker.make(
        "search.AwardSearch",
        award_id=3,
        generated_unique_award_id="UNIQUE AWARD KEY A",
        generated_unique_award_id_legacy=None,
        latest_transaction_id=434,
        earliest_transaction_search_id=434,
        latest_transaction_search_id=434,
        type_raw="A",
        type="A",
        category="contract",
        period_of_performance_start_date="2020-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2020-01-01",
        award_amount=0.00,
        total_outlays=None,
        total_obligation=0.00,
        total_subsidy_cost=0.00,
        total_obl_bin="<1M",
        last_modified_date="2020-01-01",
        update_date="2020-01-01",
        awarding_agency_id=32,
        funding_agency_id=32,
        awarding_toptier_agency_name=toptier.name,
        awarding_toptier_agency_name_raw="toptier",
        awarding_toptier_agency_code=toptier.toptier_code,
        funding_toptier_agency_name=toptier.name,
        funding_toptier_agency_name_raw="toptier",
        funding_toptier_agency_code=toptier.toptier_code,
        awarding_subtier_agency_name=subtier.name,
        awarding_subtier_agency_name_raw="subtier",
        awarding_subtier_agency_code=subtier.subtier_code,
        funding_subtier_agency_name=subtier.name,
        funding_subtier_agency_name_raw="subtier",
        funding_subtier_agency_code=subtier.subtier_code,
        funding_toptier_agency_id=agency.id,
        funding_subtier_agency_id=agency.id,
        display_award_id="PIID",
        piid="PIID",
        fain=None,
        uri=None,
        subaward_count=0,
        transaction_unique_id=434,
        is_fpds=True,
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        raw_recipient_name="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_congressional_code_current=None,
        pop_congressional_code_current=None,
        pop_country_code="USA",
        business_categories=None,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        treasury_account_identifiers=None,
        cfdas=None,
        tas_paths=None,
        tas_components=None,
        disaster_emergency_fund_codes=None,
        spending_by_defc=None,
        recipient_location_county_fips=None,
        pop_county_fips=None,
        generated_pragmatic_obligation=0.00,
        program_activities=None,
        federal_accounts=None,
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        transaction_unique_id=1,
        afa_generated_unique=1,
        action_date="2020-01-01",
        fiscal_action_date="2020-04-01",
        award_id=asst_award.award_id,
        award_amount=asst_award.total_subsidy_cost,
        generated_unique_award_id=asst_award.generated_unique_award_id,
        award_certified_date=asst_award.certified_date,
        award_fiscal_year=2020,
        fiscal_year=2020,
        award_date_signed=asst_award.date_signed,
        etl_update_date=asst_award.update_date,
        award_category=asst_award.category,
        piid=asst_award.piid,
        fain=asst_award.fain,
        uri=asst_award.uri,
        is_fpds=False,
        type_raw="07",
        type="07",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        cfda_number="12.456",
        cfda_id=cfda.id,
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        recipient_name_raw="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=0.0,
        funding_amount=0.00,
        total_funding_amount=0.00,
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        award_update_date=asst_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["L", "M"],
        recipient_location_county_fips="51001",
        pop_county_fips="51001",
        program_activities=[
            {"code": "0001", "name": "OFFICE OF THE SECRETARY"},
            {"code": "0002", "name": "OPERATIONS AND MAINTENANCE"},
        ],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        transaction_unique_id=2,
        afa_generated_unique=2,
        action_date="2020-04-01",
        fiscal_action_date="2020-07-01",
        award_id=asst_award.award_id,
        award_amount=asst_award.total_subsidy_cost,
        generated_unique_award_id=asst_award.generated_unique_award_id,
        award_certified_date=asst_award.certified_date,
        award_fiscal_year=2020,
        fiscal_year=2020,
        award_date_signed=asst_award.date_signed,
        etl_update_date=asst_award.update_date,
        award_category=asst_award.category,
        piid=asst_award.piid,
        fain=asst_award.fain,
        uri=asst_award.uri,
        is_fpds=False,
        type_raw="07",
        type="07",
        record_type=None,
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        published_fabs_id=2,
        cfda_number="12.456",
        cfda_id=cfda.id,
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        recipient_name_raw="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        award_update_date=asst_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["L", "M"],
        recipient_location_county_fips="51001",
        pop_county_fips="51001",
        program_activities=[
            {"code": "0001", "name": "OFFICE OF THE SECRETARY"},
            {"code": "0002", "name": "OPERATIONS AND MAINTENANCE"},
        ],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        transaction_unique_id=5,
        afa_generated_unique=5,
        action_date="2020-04-01",
        fiscal_action_date="2020-07-01",
        award_id=asst_award2.award_id,
        award_amount=asst_award2.total_subsidy_cost,
        generated_unique_award_id=asst_award2.generated_unique_award_id,
        award_certified_date=asst_award2.certified_date,
        award_fiscal_year=2020,
        fiscal_year=2020,
        award_date_signed=asst_award2.date_signed,
        etl_update_date=asst_award2.update_date,
        award_category=asst_award2.category,
        piid=asst_award2.piid,
        fain=asst_award2.fain,
        uri=asst_award2.uri,
        is_fpds=False,
        type_raw="02",
        type="02",
        record_type=1,
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        cfda_number="12.456",
        cfda_id=cfda.id,
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="FABS RECIPIENT 12345",
        recipient_name_raw="FABS RECIPIENT 12345",
        recipient_hash="53aea6c7-bbda-4e4b-1ebe-755157592bbf",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=0.0,
        funding_amount=0.00,
        total_funding_amount=0.00,
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_county_code="001",
        recipient_location_county_name="COUNTY NAME",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_county_code="001",
        pop_county_name="COUNTY NAME",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        recipient_location_county_population=1,
        pop_county_population=1,
        recipient_location_congressional_population=1,
        pop_congressional_population=1,
        award_update_date=asst_award2.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        treasury_account_identifiers=None,
        tas_paths=None,
        tas_components=None,
        federal_accounts=None,
        disaster_emergency_fund_codes=None,
        recipient_location_county_fips="51001",
        pop_county_fips="51001",
        program_activities=None,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        transaction_unique_id=3,
        detached_award_procurement_id=3,
        action_date="2020-07-01",
        fiscal_action_date="2020-10-01",
        award_id=cont_award.award_id,
        award_amount=cont_award.total_obligation,
        generated_unique_award_id=cont_award.generated_unique_award_id,
        award_certified_date=cont_award.certified_date,
        award_fiscal_year=2021,
        fiscal_year=2020,
        award_date_signed=cont_award.date_signed,
        etl_update_date=cont_award.update_date,
        award_category=cont_award.category,
        piid=cont_award.piid,
        fain=cont_award.fain,
        uri=cont_award.uri,
        is_fpds=True,
        type_raw="A",
        type="A",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        naics_code="123456",
        product_or_service_code="12",
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_congressional_code_current=None,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        award_update_date=cont_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["Q"],
        recipient_location_county_fips=None,
        pop_county_fips=None,
        program_activities=[{"code": "0003", "name": "TRAINING AND RECRUITING"}],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        transaction_unique_id=4,
        detached_award_procurement_id=4,
        action_date="2020-10-01",
        fiscal_action_date="2021-01-01",
        award_id=cont_award.award_id,
        award_amount=cont_award.total_obligation,
        generated_unique_award_id=cont_award.generated_unique_award_id,
        award_certified_date=cont_award.certified_date,
        award_fiscal_year=2021,
        fiscal_year=2021,
        award_date_signed=cont_award.date_signed,
        etl_update_date=cont_award.update_date,
        award_category=cont_award.category,
        piid=cont_award.piid,
        fain=cont_award.fain,
        uri=cont_award.uri,
        is_fpds=True,
        type_raw="A",
        type="A",
        awarding_agency_id=awarding_agency.id,
        funding_agency_id=funding_agency.id,
        awarding_toptier_agency_name=awarding_toptier_agency.name,
        awarding_toptier_agency_name_raw="TEST AGENCY 2",
        funding_toptier_agency_name=funding_toptier_agency.name,
        funding_toptier_agency_name_raw="TEST AGENCY 1",
        awarding_subtier_agency_name=awarding_subtier_agency.name,
        awarding_subtier_agency_name_raw="TEST SUBTIER 2",
        funding_subtier_agency_name=funding_subtier_agency.name,
        funding_subtier_agency_name_raw="TEST SUBTIER 1",
        awarding_toptier_agency_id=awarding_agency.id,
        funding_toptier_agency_id=funding_agency.id,
        awarding_toptier_agency_abbreviation=awarding_toptier_agency.abbreviation,
        funding_toptier_agency_abbreviation=funding_toptier_agency.abbreviation,
        awarding_subtier_agency_abbreviation=awarding_subtier_agency.abbreviation,
        funding_subtier_agency_abbreviation=funding_subtier_agency.abbreviation,
        last_modified_date="2020-01-01",
        federal_action_obligation=0,
        naics_code="123456",
        product_or_service_code="12",
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        parent_uei="PARENTUEI12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_code="VA",
        recipient_location_state_fips=51,
        recipient_location_state_name="Virginia",
        recipient_location_congressional_code_current=None,
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_state_code="VA",
        pop_state_fips=51,
        pop_state_name="Virginia",
        pop_congressional_code_current=None,
        recipient_location_state_population=1,
        pop_state_population=1,
        award_update_date=cont_award.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        treasury_account_identifiers=[tas.treasury_account_identifier],
        tas_paths=[
            f"agency={funding_toptier_agency.toptier_code}faaid={federal_account.agency_identifier}famain={federal_account.main_account_code}aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        tas_components=[
            f"aid={tas.agency_id}main={tas.main_account_code}ata={tas.allocation_transfer_agency_id or ''}sub={tas.sub_account_code}bpoa={tas.beginning_period_of_availability or ''}epoa={tas.ending_period_of_availability or ''}a={tas.availability_type_code}"
        ],
        federal_accounts=[
            {
                "id": federal_account.id,
                "account_title": federal_account.account_title,
                "federal_account_code": federal_account.federal_account_code,
            }
        ],
        disaster_emergency_fund_codes=["Q"],
        recipient_location_county_fips=None,
        pop_county_fips=None,
        program_activities=[{"code": "0003", "name": "TRAINING AND RECRUITING"}],
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=434,
        transaction_unique_id=434,
        detached_award_procurement_id=434,
        is_fpds=True,
        award_id=cont_award2.award_id,
        award_amount=cont_award2.total_obligation,
        generated_unique_award_id=cont_award2.generated_unique_award_id,
        award_certified_date=cont_award2.certified_date,
        etl_update_date=cont_award2.update_date,
        award_category=cont_award2.category,
        piid=cont_award2.piid,
        fain=cont_award2.fain,
        uri=cont_award2.uri,
        type_raw="A",
        type="A",
        awarding_agency_id=agency.id,
        funding_agency_id=agency.id,
        awarding_toptier_agency_name=toptier.name,
        awarding_toptier_agency_name_raw="toptier",
        funding_toptier_agency_name=toptier.name,
        funding_toptier_agency_name_raw="toptier",
        awarding_subtier_agency_name=subtier.name,
        awarding_subtier_agency_name_raw="subtier",
        funding_subtier_agency_name=subtier.name,
        funding_subtier_agency_name_raw="subtier",
        awarding_toptier_agency_abbreviation=toptier.abbreviation,
        funding_toptier_agency_abbreviation=toptier.abbreviation,
        awarding_subtier_agency_abbreviation=subtier.abbreviation,
        funding_subtier_agency_abbreviation=subtier.abbreviation,
        awarding_toptier_agency_id=agency.id,
        funding_toptier_agency_id=agency.id,
        last_modified_date="2020-01-01",
        award_update_date=cont_award2.update_date,
        generated_pragmatic_obligation=0.00,
        original_loan_subsidy_cost=0.00,
        face_value_loan_guarantee=0.00,
        non_federal_funding_amount=0.00,
        indirect_federal_sharing=0.00,
        funding_amount=0.00,
        total_funding_amount=0.00,
        federal_action_obligation=0.00,
        recipient_uei="FPDSUEI12345",
        recipient_unique_id="FPDSDUNS12345",
        recipient_name="FPDS RECIPIENT 12345",
        recipient_name_raw="FPDS RECIPIENT 12345",
        recipient_hash="f4d589f1-7921-723a-07c0-c78632748999",
        recipient_levels=["C"],
        recipient_location_congressional_code_current=None,
        pop_congressional_code_current=None,
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_hash="475752fc-dfb9-dac8-072e-3e36f630be93",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        ordering_period_end_date="2020-07-01",
        recipient_location_county_fips=None,
        pop_county_fips=None,
        program_activities=None,
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=4,
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=5,
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )

    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=6,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        submission_id=33.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=7,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        submission_id=33.00,
        _fill_optional=True,
    )

    dabs = baker.make("submissions.DABSSubmissionWindowSchedule", submission_reveal_date="2020-05-01")
    sa = baker.make(
        "submissions.SubmissionAttributes",
        reporting_period_start="2020-04-02",
        submission_window=dabs,
        is_final_balances_for_fy=True,
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=asst_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_l,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        submission=sa,
        program_activity=rpa_1,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=asst_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_m,
        submission=sa,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        program_activity=rpa_2,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=cont_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=defc_q,
        gross_outlay_amount_by_award_cpe=1,
        transaction_obligated_amount=1,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        submission=sa,
        program_activity=rpa_3,
        _fill_optional=True,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award_id=cont_award.award_id,
        treasury_account=tas,
        disaster_emergency_fund=None,
        gross_outlay_amount_by_award_cpe=2,
        transaction_obligated_amount=2,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        submission=sa,
        program_activity=rpa_3,
        _fill_optional=True,
    )


@pytest.fixture
def populate_usas_data(db):
    """Fixture to create Django model data to be saved to Postgres, which will then get ingested into Delta Lake"""
    _build_usas_data_for_spark()
    update_awards()
    yield


@pytest.fixture
def populate_usas_data_and_recipients_from_broker(db, populate_usas_data, populate_broker_data):
    with connections[settings.DEFAULT_DB_ALIAS].cursor() as cursor:
        restock_duns_sql = open("usaspending_api/broker/management/sql/restock_duns.sql", "r").read()
        restock_duns_sql = restock_duns_sql.replace("VACUUM ANALYZE int.duns;", "")
        cursor.execute(restock_duns_sql)
    call_command("update_recipient_lookup")
    with connections[settings.DEFAULT_DB_ALIAS].cursor() as cursor:
        restock_recipient_profile_sql = open(
            "usaspending_api/recipient/management/sql/restock_recipient_profile.sql", "r"
        ).read()
        cursor.execute(restock_recipient_profile_sql)
    yield


def create_all_delta_tables(spark: "SparkSession", s3_bucket: str, tables_to_load: list):
    load_query_tables = [val for val in tables_to_load if val in LOAD_QUERY_TABLE_SPEC]
    load_table_tables = [val for val in tables_to_load if val in LOAD_TABLE_TABLE_SPEC]
    for dest_table in load_table_tables + load_query_tables:
        if dest_table in [
            "awards",
            "transaction_fabs",
            "transaction_normalized",
            "transaction_fpds",
            "financial_accounts_by_awards",
        ]:
            call_command(
                "create_delta_table",
                f"--destination-table={dest_table}",
                "--alt-db=int",
                f"--spark-s3-bucket={s3_bucket}",
            )
        else:
            call_command("create_delta_table", f"--destination-table={dest_table}", f"--spark-s3-bucket={s3_bucket}")


def create_and_load_all_delta_tables(spark: "SparkSession", s3_bucket: str, tables_to_load: list):
    create_all_delta_tables(spark, s3_bucket, tables_to_load)

    load_query_tables = [val for val in tables_to_load if val in LOAD_QUERY_TABLE_SPEC]
    load_table_tables = [val for val in tables_to_load if val in LOAD_TABLE_TABLE_SPEC]

    for dest_table in load_table_tables:
        if dest_table in [
            "awards",
            "transaction_fabs",
            "transaction_normalized",
            "transaction_fpds",
            "financial_accounts_by_awards",
        ]:
            call_command(
                "load_table_to_delta",
                f"--destination-table={dest_table}",
                "--alt-db=int",
            )
        else:
            call_command(
                "load_table_to_delta",
                f"--destination-table={dest_table}",
            )

    for dest_table in load_query_tables:
        call_command("load_query_to_delta", f"--destination-table={dest_table}")

    create_ref_temp_views(spark)
