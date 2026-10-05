import logging
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DEFAULT_DB_ALIAS, connections
from model_bakery import baker
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from usaspending_api import settings
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.management.commands.load_agencies import Agency as AgencyTuple
from usaspending_api.references.management.commands.load_agencies import Command
from usaspending_api.references.models import CGAC, FREC, Agency, SubtierAgency, ToptierAgency
from usaspending_api.settings import BROKER_DB_ALIAS
from usaspending_api.transactions.models import SourceAssistanceTransaction, SourceProcurementTransaction

AGENCY_FILE = settings.APP_DIR / "references" / "tests" / "data" / "test_load_agencies.csv"
BOGUS_ABBREVIATION = "THIS IS A TEST ABBREVIATION"


@pytest.fixture
def disable_vacuuming(monkeypatch):
    """
    We cannot run vacuums in a transaction.  Since tests are run in a transaction, we'll NOOP the
    function that performs the vacuuming.
    """
    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._vacuum_tables", lambda a: None
    )


def _get_record_count():
    return (
        ToptierAgency.objects.count()
        + SubtierAgency.objects.count()
        + Agency.objects.count()
        + CGAC.objects.count()
        + FREC.objects.count()
    )


@pytest.fixture
def prepare_delta_tables(spark, s3_unittest_data_bucket, hive_unittest_metastore_db, broker_server_dblink_setup):
    tables_to_create = [
        "detached_award_procurement",
        "published_fabs",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "awards",
    ]
    for table in tables_to_create:
        call_command(
            "create_delta_table",
            f"--destination-table={table}",
            f"--spark-s3-bucket={s3_unittest_data_bucket}",
        )

    yield spark


@pytest.fixture
def transaction_test_data(prepare_delta_tables):
    try:
        spark = prepare_delta_tables

        # Create postgres data for Broker
        with connections[BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute("""
                insert into published_fabs (
                    published_fabs_id,
                    afa_generated_unique,
                    awarding_agency_code,
                    awarding_agency_name,
                    funding_agency_code,
                    funding_agency_name
                ) values (1, 'ASST_UNIQUE', '000', 'U.S. CONGRESS', '003', 'LIBRARY OF CONGRESS');
                           """)
            cursor.execute("""
                insert into detached_award_procurement (
                    detached_award_procurement_id,
                    detached_award_proc_unique,
                    awarding_agency_code,
                    awarding_agency_name,
                    funding_agency_code,
                    funding_agency_name
                ) values (
                     1,
                     'CONT_UNIQUE',
                     '005',
                     'GOVERNMENT ACCOUNTABILITY OFFICE',
                     '008',
                     'CONGRESSIONAL BUDGET OFFICE'
                );""")

        # Create postgres data for USAspending
        baker.make(
            "search.TransactionSearch",
            transaction_id=1,
            published_fabs_id=1,
            is_fpds=False,
            awarding_agency_code="999",
            funding_agency_code="999",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=2,
            detached_award_procurement_id=1,
            is_fpds=True,
            awarding_agency_code="999",
            funding_agency_code="999",
        )
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=1,
            awarding_agency_code="999",
            awarding_agency_name=None,
            funding_agency_code="999",
            funding_agency_name=None,
        )
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=1,
            awarding_agency_code="999",
            awarding_agency_name=None,
            funding_agency_code="999",
            funding_agency_name=None,
        )

        # Create the delta data for USAspending
        published_fabs_df = spark.createDataFrame(
            [
                (1, "999", None, "999", None),
            ],
            schema=StructType(
                [
                    StructField("published_fabs_id", IntegerType()),
                    StructField("awarding_agency_code", StringType()),
                    StructField("awarding_agency_name", StringType()),
                    StructField("funding_agency_code", StringType()),
                    StructField("funding_agency_name", StringType()),
                ]
            ),
        )
        detached_award_procurement_df = spark.createDataFrame(
            [
                (1, "999", None, "999", None),
            ],
            schema=StructType(
                [
                    StructField("detached_award_procurement_id", IntegerType()),
                    StructField("awarding_agency_code", StringType()),
                    StructField("awarding_agency_name", StringType()),
                    StructField("funding_agency_code", StringType()),
                    StructField("funding_agency_name", StringType()),
                ]
            ),
        )
        transaction_fabs_df = spark.createDataFrame(
            [
                (1, 1, "999", None, "0010", "999", None, "0300"),
            ],
            schema=StructType(
                [
                    StructField("transaction_id", LongType()),
                    StructField("published_fabs_id", IntegerType()),
                    StructField("awarding_agency_code", StringType()),
                    StructField("awarding_agency_name", StringType()),
                    StructField("awarding_sub_tier_agency_c", StringType()),
                    StructField("funding_agency_code", StringType()),
                    StructField("funding_agency_name", StringType()),
                    StructField("funding_sub_tier_agency_co", StringType()),
                ]
            ),
        )
        transaction_fpds_df = spark.createDataFrame(
            [
                (2, 1, "999", None, "0500", "999", None, "0800"),
            ],
            schema=StructType(
                [
                    StructField("transaction_id", LongType()),
                    StructField("detached_award_procurement_id", IntegerType()),
                    StructField("awarding_agency_code", StringType()),
                    StructField("awarding_agency_name", StringType()),
                    StructField("awarding_sub_tier_agency_c", StringType()),
                    StructField("funding_agency_code", StringType()),
                    StructField("funding_agency_name", StringType()),
                    StructField("funding_sub_tier_agency_co", StringType()),
                ]
            ),
        )
        transaction_normalized_df = spark.createDataFrame(
            [(1, 1, "ASST_UNIQUE", None, None, False), (2, 2, "CONT_UNIQUE", None, None, True)],
            schema=StructType(
                [
                    StructField("id", LongType()),
                    StructField("award_id", LongType()),
                    StructField("transaction_unique_id", StringType()),
                    StructField("awarding_agency_id", IntegerType()),
                    StructField("funding_agency_id", IntegerType()),
                    StructField("is_fpds", BooleanType()),
                ]
            ),
        )
        awards_df = spark.createDataFrame(
            [
                (1, 1, "ASST_AWARD_UNIQUE", "ASST_UNIQUE", None, None, False, 0, None),
                (2, 2, "CONT_AWARD_UNIQUE", "CONT_UNIQUE", None, None, True, 0, None),
            ],
            schema=StructType(
                [
                    StructField("id", LongType()),
                    StructField("latest_transaction_id", LongType()),
                    StructField("generated_unique_award_id", StringType()),
                    StructField("transaction_unique_id", StringType()),
                    StructField("awarding_agency_id", IntegerType()),
                    StructField("funding_agency_id", IntegerType()),
                    StructField("is_fpds", BooleanType()),
                    StructField("subaward_count", IntegerType()),
                    StructField("update_date", TimestampType()),
                ]
            ),
        )

        published_fabs_df.write.format("delta").mode("append").saveAsTable("raw.published_fabs")
        detached_award_procurement_df.write.format("delta").mode("append").saveAsTable("raw.detached_award_procurement")
        transaction_fabs_df.write.format("delta").mode("append").saveAsTable("int.transaction_fabs")
        transaction_fpds_df.write.format("delta").mode("append").saveAsTable("int.transaction_fpds")
        transaction_normalized_df.write.format("delta").mode("append").saveAsTable("int.transaction_normalized")
        awards_df.write.format("delta").mode("append").saveAsTable("int.awards")

        yield spark
    finally:
        # Manually clean up the Broker test data just in case
        connection = connections[settings.BROKER_DB_ALIAS]
        with connection.cursor() as cursor:
            cursor.execute(
                """
                truncate table published_fabs restart identity cascade;
                truncate table detached_award_procurement restart identity cascade;
                """
            )


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)(transaction=True)
def test_happy_path(prepare_delta_tables):
    """
    We're running this one without a transaction just to ensure the vacuuming doesn't blow up.  For
    the remaining tests we'll run inside of a transaction since it's faster.
    """

    # Confirm everything is empty.
    assert CGAC.objects.count() == 0
    assert FREC.objects.count() == 0
    assert SubtierAgency.objects.count() == 0
    assert ToptierAgency.objects.count() == 0
    assert Agency.objects.count() == 0

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Confirm nothing is empty.
    assert CGAC.objects.count() > 0
    assert FREC.objects.count() > 0
    assert SubtierAgency.objects.count() > 0
    assert ToptierAgency.objects.count() > 0
    assert Agency.objects.count() > 0


@pytest.mark.django_db
def test_no_file_provided():
    # This should error since agency file is required.
    with pytest.raises(CommandError):
        call_command("load_agencies")


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_create_agency(disable_vacuuming, monkeypatch, prepare_delta_tables):
    """Let's add an agency record to the "raw" file and see what happens."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    record_count = _get_record_count()

    original_read_raw_agencies_csv = Command._read_raw_agencies_csv

    def add_agency(self):
        original_read_raw_agencies_csv(self)
        self.agencies.append(
            AgencyTuple(
                row_number=len(self.agencies) + 1,
                cgac_agency_code="123",
                agency_name="BOGUS CGAC NAME",
                agency_abbreviation="BOGUS CGAC ABBREVIATION",
                frec="4567",
                frec_entity_description="BOGUS FREC NAME",
                frec_abbreviation="BOGUS FREC ABBREVIATION",
                subtier_code="8901",
                subtier_name="BOGUS SUBTIER NAME",
                subtier_abbreviation="BOGUS SUBTIER ABBREVIATION",
                toptier_flag=True,
                is_frec=False,
                frec_cgac_association=False,
                user_selectable=True,
                mission="BOGUS MISSION",
                about_agency_data="BOGUS ABOUT AGENCY DATA",
                website="BOGUS WEBSITE",
                congressional_justification="BOGUS CONGRESSIONAL JUSTIFICATION",
                icon_filename="BOGUS ICON FILENAME",
            )
        )

    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._read_raw_agencies_csv", add_agency
    )

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # 1 toptier + 1 subtier + 1 agency + 1 CGAC + 1 FREC = 5 things
    assert _get_record_count() == record_count + 5


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_update_agency(disable_vacuuming, prepare_delta_tables):
    """Also confirm agency data is updated in place instead of being recreated as a new record."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    record_count = _get_record_count()

    # Grab a toptier, a subtier, and an agency.
    toptier_agency = ToptierAgency.objects.first()
    subtier_agency = SubtierAgency.objects.first()
    agency = Agency.objects.first()

    # Make a change to each.
    ToptierAgency.objects.filter(pk=toptier_agency.pk).update(abbreviation=BOGUS_ABBREVIATION)
    SubtierAgency.objects.filter(pk=subtier_agency.pk).update(abbreviation=BOGUS_ABBREVIATION)
    Agency.objects.filter(pk=agency.pk).update(toptier_flag=(not agency.toptier_flag))

    # Confirm our changes took.  (Yes, we're testing our tests.)
    assert ToptierAgency.objects.get(pk=toptier_agency.pk).abbreviation == BOGUS_ABBREVIATION
    assert SubtierAgency.objects.get(pk=subtier_agency.pk).abbreviation == BOGUS_ABBREVIATION
    assert Agency.objects.get(pk=agency.pk).toptier_flag == (not agency.toptier_flag)

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Confirm our changes were reverted.  Coincidentally, this also confirms that our ids didn't change
    # which means our records were updated in place instead of being recreated since "get" would blow up
    # if the ids were different.
    assert ToptierAgency.objects.get(pk=toptier_agency.pk).abbreviation == toptier_agency.abbreviation
    assert SubtierAgency.objects.get(pk=subtier_agency.pk).abbreviation == subtier_agency.abbreviation
    assert Agency.objects.get(pk=agency.pk).toptier_flag == agency.toptier_flag

    # Ensure nothing new was added.
    assert record_count == _get_record_count()


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_delete_agency(disable_vacuuming, monkeypatch, prepare_delta_tables):
    """Let's remove an entire toptier agency and see what happens."""

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    toptier_count = ToptierAgency.objects.count()
    subtier_count = SubtierAgency.objects.count()
    agency_count = Agency.objects.count()
    cgac_count = CGAC.objects.count()
    frec_count = FREC.objects.count()

    original_read_raw_agencies_csv = Command._read_raw_agencies_csv

    def remove_toptier_agency(self):
        original_read_raw_agencies_csv(self)
        toptier_code = self.agencies[0].cgac_agency_code
        self.agencies = [a for a in self.agencies if a.cgac_agency_code != toptier_code]

    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_agencies.Command._read_raw_agencies_csv",
        remove_toptier_agency,
    )

    # Reload all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Make sure the data was affected as expected.
    assert ToptierAgency.objects.count() == toptier_count - 1
    assert SubtierAgency.objects.count() == subtier_count - 3
    assert Agency.objects.count() == agency_count - 3
    assert CGAC.objects.count() == cgac_count - 1
    assert FREC.objects.count() == frec_count  # This frec is associated with more than one agency in our test data


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_update_treasury_appropriation_account(disable_vacuuming, prepare_delta_tables):
    # Create a bogus TAS.
    baker.make("accounts.TreasuryAppropriationAccount", agency_id="009")

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE)

    # Ensure our TAS got updated.
    assert (
        TreasuryAppropriationAccount.objects.first().funding_toptier_agency_id
        == ToptierAgency.objects.get(toptier_code="009").toptier_agency_id
    )

    # Set it to something else and reload to make sure it gets updated.
    TreasuryAppropriationAccount.objects.update(agency_id="005")

    # Double check.
    assert TreasuryAppropriationAccount.objects.first().agency_id == "005"

    # So there's a cutoff in the code to skip "expensive" steps if the agency data didn't
    # change.  Let's make a small tweak to the agency data to ensure TAS gets updated.
    ToptierAgency.objects.update(abbreviation=BOGUS_ABBREVIATION)

    # Reload the agency file.
    call_command("load_agencies", AGENCY_FILE)

    # Was it fixed?
    assert (
        TreasuryAppropriationAccount.objects.first().funding_toptier_agency_id
        == ToptierAgency.objects.get(toptier_code="005").toptier_agency_id
    )


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_update_transactions_and_awards(disable_vacuuming, transaction_test_data, caplog, monkeypatch):
    """Test both together since they're so tightly intertwined."""
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("usaspending_api.references.management.commands.load_agencies.logger", logging.getLogger())
    spark = transaction_test_data

    # Load all the things.
    call_command("load_agencies", AGENCY_FILE, "--file-d-dry-run")

    caplog_messages = [rec.message for rec in caplog.records]
    assert "1 record(s) would be updated in raw.source_assistance_transaction" in caplog_messages
    assert "1 record(s) would be updated in raw.source_procurement_transaction" in caplog_messages
    assert "1 record(s) would be updated in raw.published_fabs" in caplog_messages
    assert "1 record(s) would be updated in raw.detached_award_procurement" in caplog_messages
    assert "1 record(s) would be updated in int.transaction_fabs" in caplog_messages
    assert "1 record(s) would be updated in int.transaction_fpds" in caplog_messages
    assert "2 record(s) would be updated in int.transaction_normalized" in caplog_messages
    assert "2 record(s) would be updated in int.awards" in caplog_messages

    mocked_datetime_now = datetime(2026, 1, 1, 0, 0, 0, 0, timezone.utc)
    with patch("usaspending_api.references.management.commands.load_agencies.datetime") as mock_datetime:
        mock_datetime.now.return_value = mocked_datetime_now
        # Call again but force to pick up File D changes following a dry-run
        call_command("load_agencies", AGENCY_FILE, "--force")

    # Validate postgres tables
    source_assistance_transaction = SourceAssistanceTransaction.objects.first()
    assert source_assistance_transaction.awarding_agency_code == "000"
    assert source_assistance_transaction.awarding_agency_name == "U.S. CONGRESS"
    assert source_assistance_transaction.funding_agency_code == "003"
    assert source_assistance_transaction.funding_agency_name == "LIBRARY OF CONGRESS"

    source_procurement_transaction = SourceProcurementTransaction.objects.first()
    assert source_procurement_transaction.awarding_agency_code == "005"
    assert source_procurement_transaction.awarding_agency_name == "GOVERNMENT ACCOUNTABILITY OFFICE"
    assert source_procurement_transaction.funding_agency_code == "008"
    assert source_procurement_transaction.funding_agency_name == "CONGRESSIONAL BUDGET OFFICE"

    # Get agencies for validation
    fabs_awarding_agency_id = Agency.objects.get(subtier_agency__subtier_code="0010").id
    fabs_funding_agency_id = Agency.objects.get(subtier_agency__subtier_code="0300").id
    fpds_awarding_agency_id = Agency.objects.get(subtier_agency__subtier_code="0500").id
    fpds_funding_agency_id = Agency.objects.get(subtier_agency__subtier_code="0800").id

    # Get dataframes for each table to validate
    published_fabs_df = spark.table("raw.published_fabs")
    detached_award_procurement_df = spark.table("raw.detached_award_procurement")
    transaction_fabs_df = spark.table("int.transaction_fabs")
    transaction_fpds_df = spark.table("int.transaction_fpds")
    transaction_normalized_df = spark.table("int.transaction_normalized")
    awards_df = spark.table("int.awards")

    published_fabs_record = published_fabs_df.filter(published_fabs_df["published_fabs_id"] == 1).first()
    assert published_fabs_record["awarding_agency_code"] == "000"
    assert published_fabs_record["awarding_agency_name"] == "U.S. CONGRESS"
    assert published_fabs_record["funding_agency_code"] == "003"
    assert published_fabs_record["funding_agency_name"] == "LIBRARY OF CONGRESS"

    detached_award_procurement_record = detached_award_procurement_df.filter(
        detached_award_procurement_df["detached_award_procurement_id"] == 1
    ).first()
    assert detached_award_procurement_record["awarding_agency_code"] == "005"
    assert detached_award_procurement_record["awarding_agency_name"] == "GOVERNMENT ACCOUNTABILITY OFFICE"
    assert detached_award_procurement_record["funding_agency_code"] == "008"
    assert detached_award_procurement_record["funding_agency_name"] == "CONGRESSIONAL BUDGET OFFICE"

    transaction_fabs_record = transaction_fabs_df.filter(transaction_fabs_df["published_fabs_id"] == 1).first()
    assert transaction_fabs_record["awarding_agency_code"] == "000"
    assert transaction_fabs_record["awarding_agency_name"] == "U.S. CONGRESS"
    assert transaction_fabs_record["funding_agency_code"] == "003"
    assert transaction_fabs_record["funding_agency_name"] == "LIBRARY OF CONGRESS"

    transaction_fpds_record = transaction_fpds_df.filter(
        transaction_fpds_df["detached_award_procurement_id"] == 1
    ).first()
    assert transaction_fpds_record["awarding_agency_code"] == "005"
    assert transaction_fpds_record["awarding_agency_name"] == "GOVERNMENT ACCOUNTABILITY OFFICE"
    assert transaction_fpds_record["funding_agency_code"] == "008"
    assert transaction_fpds_record["funding_agency_name"] == "CONGRESSIONAL BUDGET OFFICE"

    asst_transaction_normalized_record = transaction_normalized_df.filter(transaction_normalized_df["id"] == 1).first()
    cont_transaction_normalized_record = transaction_normalized_df.filter(transaction_normalized_df["id"] == 2).first()
    assert asst_transaction_normalized_record["awarding_agency_id"] == fabs_awarding_agency_id
    assert asst_transaction_normalized_record["funding_agency_id"] == fabs_funding_agency_id
    assert cont_transaction_normalized_record["awarding_agency_id"] == fpds_awarding_agency_id
    assert cont_transaction_normalized_record["funding_agency_id"] == fpds_funding_agency_id

    asst_awards_record = awards_df.filter(awards_df["id"] == 1).first()
    cont_awards_record = awards_df.filter(awards_df["id"] == 2).first()
    expected_update_date = datetime(2026, 1, 1, 0, 0)
    assert asst_awards_record["awarding_agency_id"] == fabs_awarding_agency_id
    assert asst_awards_record["funding_agency_id"] == fabs_funding_agency_id
    assert asst_awards_record["update_date"] == expected_update_date
    assert cont_awards_record["awarding_agency_id"] == fpds_awarding_agency_id
    assert cont_awards_record["funding_agency_id"] == fpds_funding_agency_id
    assert cont_awards_record["update_date"] == expected_update_date


@pytest.mark.django_db(databases=[BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_exceeding_max_changes(disable_vacuuming, monkeypatch, prepare_delta_tables):
    """
    We have a safety cutoff to prevent accidentally updating every record in the database.  Make
    sure we get an exception if we exceed that threshold.
    """
    monkeypatch.setattr("usaspending_api.references.management.commands.load_agencies.MAX_CHANGES", 1)

    with pytest.raises(RuntimeError):
        call_command("load_agencies", AGENCY_FILE)

    # Confirm everything is still empty.
    assert CGAC.objects.count() == 0
    assert FREC.objects.count() == 0
    assert SubtierAgency.objects.count() == 0
    assert ToptierAgency.objects.count() == 0
    assert Agency.objects.count() == 0

    call_command("load_agencies", "--force", AGENCY_FILE)

    # Confirm nothing is empty.
    assert CGAC.objects.count() > 0
    assert FREC.objects.count() > 0
    assert SubtierAgency.objects.count() > 0
    assert ToptierAgency.objects.count() > 0
    assert Agency.objects.count() > 0
