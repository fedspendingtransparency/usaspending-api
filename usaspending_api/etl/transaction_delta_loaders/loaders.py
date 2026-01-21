import copy
import logging
from abc import ABC
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Generator, Literal

from delta import DeltaTable
from pyspark.sql import functions as sf, SparkSession, Window
from pyspark.sql.types import ArrayType, StringType

from usaspending_api.broker.helpers.build_business_categories_boolean_dict import fpds_boolean_columns
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import (
    get_earliest_load_date,
    update_last_load_date,
)
from usaspending_api.common.data_classes import TransactionColumn
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)

from usaspending_api.transactions.delta_models.transaction_fabs import (
    FABS_TO_NORMALIZED_COLUMN_INFO,
    TRANSACTION_FABS_COLUMN_INFO,
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    DAP_TO_NORMALIZED_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMN_INFO,
)
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS

logger = logging.getLogger(__name__)


class AbstractDeltaTransactionLoader(ABC):

    spark: SparkSession
    id_col: str
    source_table: str
    col_info = list[TransactionColumn]

    def __init__(self, etl_level: Literal["fabs", "fpds", "normalized"], spark_s3_bucket: str) -> None:
        self.etl_level = etl_level
        self.spark_s3_bucket: spark_s3_bucket

    def load_transactions(self) -> None:
        with self.prepare_spark():
            if not self.spark._jsparkSession.catalog().tableExists(f"int.transaction_{self.etl_level}"):
                raise Exception(f"Table: int.transaction_{self.etl_level} does not exist.")
            logger.info(f"Running UPSERT SQL for transaction_{self.etl_level} ETL")
            self.spark.sql(self.transaction_merge_into_sql())
            next_last_load = get_earliest_load_date(
                ("source_procurement_transaction", "source_assistance_transaction"), datetime.utcfromtimestamp(0)
            )
            update_last_load_date(f"transaction_{self.etl_level}", next_last_load)

    @contextmanager
    def prepare_spark(self) -> Generator[None, None, None]:
        extra_conf = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        # Create the Spark Session
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        # Create UDFs for Business Categories
        self.spark.udf.register(
            name="get_business_categories_fabs", f=get_business_categories_fabs, returnType=ArrayType(StringType())
        )
        self.spark.udf.register(
            name="get_business_categories_fpds", f=get_business_categories_fpds, returnType=ArrayType(StringType())
        )

        yield  # Going to wait for the Django command to complete then stop the spark session if needed

        if spark_created_by_command:
            self.spark.stop()

    def build_date_format_sql(self, col: TransactionColumn, is_casted_to_date: bool = True) -> str:
        # Each of these regexps allows for an optional timestamp portion, separated from the date by some character,
        #   and the timestamp allows for an optional UTC offset.  In any case, the timestamp is ignored, though.
        regexp_mmddYYYY = r"(\\d{2})(?<sep>[-/])(\\d{2})(\\k<sep>)(\\d{4})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"
        regexp_YYYYmmdd = r"(\\d{4})(?<sep>[-/]?)(\\d{2})(\\k<sep>)(\\d{2})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"

        mmddYYYY_fmt = f"""
            (regexp_extract({self.source_table}.{col.source}, '{regexp_mmddYYYY}', 5)
            || '-' ||
            regexp_extract({self.source_table}.{col.source}, '{regexp_mmddYYYY}', 1)
            || '-' ||
            regexp_extract({self.source_table}.{col.source}, '{regexp_mmddYYYY}', 3))
        """
        YYYYmmdd_fmt = f"""
            (regexp_extract({self.source_table}.{col.source}, '{regexp_YYYYmmdd}', 1)
            || '-' ||
            regexp_extract({self.source_table}.{col.source}, '{regexp_YYYYmmdd}', 3)
            || '-' ||
            regexp_extract({self.source_table}.{col.source}, '{regexp_YYYYmmdd}', 5))
        """

        if is_casted_to_date:
            mmddYYYY_fmt = f"""CAST({mmddYYYY_fmt}
                        AS DATE)
            """
            YYYYmmdd_fmt = f"""CAST({YYYYmmdd_fmt}
                        AS DATE)
            """

        sql_snippet = f"""
            CASE WHEN regexp({self.source_table}.{col.source}, '{regexp_mmddYYYY}')
                      THEN {mmddYYYY_fmt}
                 ELSE {YYYYmmdd_fmt}
            END
        """

        return sql_snippet

    def handle_column(self, col: TransactionColumn, is_result_aliased=True) -> str:
        if col.handling == "cast":
            retval = f"CAST({self.source_table}.{col.source} AS {col.delta_type})"
        elif col.handling == "literal":
            # Use col.source directly as the value
            retval = f"{col.source}"
        elif col.handling == "parse_string_datetime_to_date":
            # These are string fields that actually hold DATES/TIMESTAMPS and need to be cast as dates.
            # However, they may not be properly parsed when calling CAST(... AS DATE).
            retval = self.build_date_format_sql(col, is_casted_to_date=True)
        elif col.handling == "string_datetime_remove_timestamp":
            # These are string fields that actually hold DATES/TIMESTAMPS, but need the non-DATE part discarded,
            # even though they remain as strings
            retval = self.build_date_format_sql(col, is_casted_to_date=False)
        elif col.delta_type.upper() == "STRING":
            # Capitalize and remove leading & trailing whitespace from all string values
            retval = f"ucase(trim({self.source_table}.{col.source}))"
        elif col.delta_type.upper() == "BOOLEAN" and not col.handling == "leave_null":
            # Unless specified, convert any nulls to false for boolean columns
            retval = f"COALESCE({self.source_table}.{col.source}, FALSE)"
        else:
            retval = f"{self.source_table}.{col.source}"

        # Handle scalar transformations if the column requires it
        if col.scalar_transformation is not None:
            retval = col.scalar_transformation.format(input=retval)

        retval = f"{retval}{' AS ' + col.dest_name if is_result_aliased else ''}"
        return retval

    @property
    def select_columns(self) -> list[str]:
        return ["CAST(NULL AS LONG) AS transaction_id"] + [
            self.handle_column(col) for col in self.col_info if col.dest_name != "transaction_id"
        ]

    def source_subquery_sql(self) -> str:
        select_columns_str = ",\n    ".join(self.select_columns)
        sql = f"""
            SELECT
                {select_columns_str}
            FROM {self.source_table}
        """
        return sql

    def transaction_merge_into_sql(self) -> str:
        silver_table_cols = ", ".join([col.dest_name for col in self.col_info if col.dest_name != "transaction_id"])
        sql = f"""
            MERGE INTO int.transaction_{self.etl_level} AS silver_table
            USING (
                {self.source_subquery_sql()}
            ) AS source_subquery
            ON
                silver_table.{self.id_col} = source_subquery.{self.id_col}
                AND silver_table.hash = source_subquery.hash
            WHEN NOT MATCHED
                THEN INSERT
                    ({silver_table_cols})
                    VALUES ({silver_table_cols})
            WHEN NOT MATCHED BY SOURCE
                THEN DELETE
        """

        return sql


class FPDSDeltaTransactionLoader(AbstractDeltaTransactionLoader):

    def __init__(self, spark_s3_bucket: str) -> None:
        super().__init__(etl_level="fpds", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "detached_award_proc_unique"
        self.source_table = "raw.detached_award_procurement"
        self.col_info = TRANSACTION_FPDS_COLUMN_INFO


class FABSDeltaTransactionLoader(AbstractDeltaTransactionLoader):

    def __init__(self, spark_s3_bucket: str) -> None:
        super().__init__(etl_level="fabs", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "afa_generated_unique"
        self.source_table = "raw.published_fabs"
        self.col_info = TRANSACTION_FABS_COLUMN_INFO


class NormalizedMixin:

    spark: SparkSession
    handle_column: Callable
    source_table: str
    etl_level: str
    select_columns: list[str]
    to_normalized_col_info: list[TransactionColumn]
    normalization_type: Literal["fabs", "fpds"]
    prepare_spark: Callable

    def source_subquery_sql(self) -> str:
        additional_joins = f"""
            LEFT OUTER JOIN global_temp.subtier_agency AS funding_subtier_agency ON (
                funding_subtier_agency.subtier_code = {self.source_table}.funding_sub_tier_agency_co
            )
            LEFT OUTER JOIN global_temp.agency AS funding_agency ON (
                funding_agency.subtier_agency_id = funding_subtier_agency.subtier_agency_id
            )
            LEFT OUTER JOIN global_temp.subtier_agency AS awarding_subtier_agency ON (
                awarding_subtier_agency.subtier_code = {self.source_table}.awarding_sub_tier_agency_c
            )
            LEFT OUTER JOIN global_temp.agency AS awarding_agency ON (
                awarding_agency.subtier_agency_id = awarding_subtier_agency.subtier_agency_id
            )
        """

        # Since the select columns may have complicated logic, put them on separate lines for debugging.
        # However, strings inside {} expressions in f-strings can't contain backslashes, so will join them first
        # before inserting into overall sql statement.
        select_columns_str = ",\n    ".join(self.select_columns)
        return f"""
            SELECT
                {select_columns_str}
            FROM {self.source_table}
            {additional_joins}
        """

    def transaction_merge_into_sql(self) -> str:
        create_ref_temp_views(self.spark)
        load_datetime = datetime.now(timezone.utc)
        special_columns = ["create_date", "update_date"]
        # On set, create_date will not be changed and update_date will be set below.  All other column
        # values will come from the subquery.
        set_cols = [
            f"int.transaction_normalized.{col_name} = source_subquery.{col_name}"
            for col_name in TRANSACTION_NORMALIZED_COLUMNS
            if col_name not in special_columns
        ]
        set_cols.append(f"""int.transaction_normalized.update_date = '{load_datetime.isoformat(" ")}'""")
        # Move create_date and update_date to the end of the list of column names for ease of handling
        # during record insert
        insert_col_name_list = [
            col_name for col_name in TRANSACTION_NORMALIZED_COLUMNS if col_name not in special_columns
        ]
        insert_col_name_list.extend(special_columns)
        insert_col_names = ", ".join([col_name for col_name in insert_col_name_list])

        # On insert, all values except for create_date and update_date will come from the subquery
        insert_value_list = insert_col_name_list[:-2]
        insert_value_list.extend([f"""'{load_datetime.isoformat(" ")}'"""] * 2)
        insert_values = ", ".join([value for value in insert_value_list])

        sql = f"""
            MERGE INTO int.transaction_normalized
            USING (
                {self.source_subquery_sql()}
            ) AS source_subquery
            ON transaction_normalized.transaction_unique_id = source_subquery.transaction_unique_id
                AND transaction_normalized.hash = source_subquery.hash
            WHEN NOT MATCHED
                THEN INSERT
                    ({insert_col_names})
                    VALUES ({insert_values})
            WHEN NOT MATCHED BY SOURCE AND {'NOT' if self.normalization_type== 'fabs' else ''} transaction_normalized.is_fpds
                THEN DELETE
        """

        return sql

    def populate_transaction_normalized_ids(self) -> None:
        target = DeltaTable.forName(self.spark, "int.transaction_normalized").alias("t")
        tn = self.spark.table("int.transaction_normalized")
        needs_ids = tn.filter(tn.id.isNull())
        if not needs_ids.isEmpty():
            max_id = tn.agg(sf.max("id")).collect()[0][0]
            max_id = max_id if max_id else 0
            w = Window.orderBy(needs_ids.transaction_unique_id)
            with_ids = needs_ids.withColumn("id", (max_id + sf.row_number().over(w)).cast("LONG")).alias("s")
            (
                target.merge(with_ids, "t.transaction_unique_id = s.transaction_unique_id AND t.hash = s.hash")
                .whenMatchedUpdateAll()
                .execute()
            )

    def link_transactions_to_normalized(self) -> None:
        tn = self.spark.table("int.transaction_normalized")
        tablename = f"int.transaction_{self.normalization_type}"
        id_col = "detached_award_proc_unique" if self.normalization_type == "fpds" else "afa_generated_unique"
        target = DeltaTable.forName(self.spark, tablename).alias("t")
        source = self.spark.table(tablename)
        needs_ids = (
            source.join(
                tn,
                on=(
                    (tn.transaction_unique_id == source[id_col])
                    & (tn.hash == source.hash)
                    & (source.transaction_id.isNull() | (source.transaction_id != tn.id))
                ),
                how="inner",
            )
            .select(tn.id, source[id_col], source.hash)
            .alias("s")
        )
        (
            target.merge(needs_ids, f"t.{id_col} = s.{id_col} AND t.hash = s.hash")
            .whenMatchedUpdate(set={"t.transaction_id": "s.id"})
            .execute()
        )

    def populate_award_ids(self) -> None:
        awards = self.spark.table("int.awards")
        max_id = awards.agg(sf.max("id")).collect()[0][0]
        max_id = max_id if max_id else 0
        target = DeltaTable.forName(self.spark, "int.transaction_normalized").alias("t")
        source = self.spark.table("int.transaction_normalized")
        needs_ids = (
            source.join(awards, awards.generated_unique_award_id == source.unique_award_key, how="left")
            .filter(awards.id.isNull())
            .select(source.unique_award_key)
            .distinct()
        )
        logger.info(f"Found {needs_ids.count()} awards that need ids")
        w = Window.orderBy(needs_ids.unique_award_key)
        with_ids = needs_ids.withColumn("award_id", (max_id + sf.row_number().over(w)).cast("LONG")).alias("s")
        logger.info(f"generated {with_ids.count()} award_id's")
        logger.info(with_ids.show(10))
        (
            target.merge(with_ids, f"t.unique_award_key = s.unique_award_key")
            .whenMatchedUpdate(set={"t.award_id": "s.award_id"})
            .execute()
        )

    def load_transactions(self) -> None:
        super().load_transactions()
        with self.prepare_spark():
            self.populate_award_ids()
            self.populate_transaction_normalized_ids()
            self.link_transactions_to_normalized()


class FABSNormalizedDeltaTransactionLoader(NormalizedMixin, AbstractDeltaTransactionLoader):

    def __init__(self, spark_s3_bucket: str) -> None:
        super().__init__(etl_level="normalized", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "transaction_unique_id"
        self.source_table = "raw.published_fabs"
        self.to_normalized_col_info = FABS_TO_NORMALIZED_COLUMN_INFO
        self.normalization_type = "fabs"

    @property
    def select_columns(self) -> list[str]:
        action_date_col = next(
            filter(lambda c: c.dest_name == "action_date" and c.source == "action_date", FABS_TO_NORMALIZED_COLUMN_INFO)
        )
        parse_action_date_sql_snippet = self.handle_column(action_date_col, is_result_aliased=False)
        select_cols = [
            "CAST(NULL AS LONG) AS id",
            "CAST(NULL AS LONG) AS award_id",
            "awarding_agency.id AS awarding_agency_id",
            f"""CASE WHEN month({parse_action_date_sql_snippet}) > 9
                    THEN year({parse_action_date_sql_snippet}) + 1
                    ELSE year({parse_action_date_sql_snippet})
                END AS fiscal_year""",
            "funding_agency.id AS funding_agency_id",
        ]
        select_cols.extend(
            [
                # business_categories
                f"get_business_categories_fabs({self.source_table}.business_types) AS business_categories",
                # funding_amount
                # In theory, this should be equal to
                #   CAST(COALESCE({bronze_table_name}.federal_action_obligation, 0)
                #        + COALESCE({bronze_table_name}.non_federal_funding_amount, 0)
                #        AS NUMERIC(23, 2))
                #   However, for some historical records, this isn't true.
                f"""
                    CAST({self.source_table}.total_funding_amount AS NUMERIC(23, 2)) AS funding_amount
                    """,
            ]
        )

        for col in FABS_TO_NORMALIZED_COLUMN_INFO:
            select_cols.append(self.handle_column(col))
        return select_cols


class FPDSNormalizedDeltaTransactionLoader(NormalizedMixin, AbstractDeltaTransactionLoader):

    def __init__(self, spark_s3_bucket: str) -> None:
        super().__init__(etl_level="normalized", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "transaction_unique_id"
        self.source_table = "raw.detached_award_procurement"
        self.to_normalized_col_info = DAP_TO_NORMALIZED_COLUMN_INFO
        self.normalization_type = "fpds"

    @property
    def select_columns(self) -> list[str]:
        action_date_col = next(
            filter(lambda c: c.dest_name == "action_date" and c.source == "action_date", DAP_TO_NORMALIZED_COLUMN_INFO)
        )
        parse_action_date_sql_snippet = self.handle_column(action_date_col, is_result_aliased=False)
        select_cols = [
            "CAST(NULL AS LONG) AS id",
            "CAST(NULL AS LONG) AS award_id",
            "awarding_agency.id AS awarding_agency_id",
            f"""CASE WHEN month({parse_action_date_sql_snippet}) > 9
                    THEN year({parse_action_date_sql_snippet}) + 1
                    ELSE year({parse_action_date_sql_snippet})
                END AS fiscal_year""",
            "funding_agency.id AS funding_agency_id",
        ]
        fpds_business_category_columns = copy.copy(fpds_boolean_columns)
        # Add a couple of non-boolean columns that are needed in the business category logic
        fpds_business_category_columns.extend(["contracting_officers_deter", "domestic_or_foreign_entity"])
        named_struct_text = ", ".join([f"'{col}', {self.source_table}.{col}" for col in fpds_business_category_columns])
        select_cols.extend(
            [
                # business_categories
                f"get_business_categories_fpds(named_struct({named_struct_text})) AS business_categories",
                # type
                f"""
                    CASE WHEN {self.source_table}.pulled_from <> 'IDV' THEN {self.source_table}.contract_award_type
                         WHEN {self.source_table}.idv_type = 'B' AND {self.source_table}.type_of_idc IS NOT NULL
                           THEN 'IDV_B_' || {self.source_table}.type_of_idc
                         WHEN {self.source_table}.idv_type = 'B'
                             AND {self.source_table}.type_of_idc_description = 'INDEFINITE DELIVERY / REQUIREMENTS'
                           THEN 'IDV_B_A'
                         WHEN {self.source_table}.idv_type = 'B'
                             AND {self.source_table}.type_of_idc_description =
                                 'INDEFINITE DELIVERY / INDEFINITE QUANTITY'
                           THEN 'IDV_B_B'
                         WHEN {self.source_table}.idv_type = 'B'
                             AND {self.source_table}.type_of_idc_description =
                                 'INDEFINITE DELIVERY / DEFINITE QUANTITY'
                           THEN 'IDV_B_C'
                         ELSE 'IDV_' || {self.source_table}.idv_type
                    END AS type
                    """,
                # type_description
                f"""
                    CASE WHEN {self.source_table}.pulled_from <> 'IDV'
                           THEN {self.source_table}.contract_award_type_desc
                         WHEN {self.source_table}.idv_type = 'B'
                             AND {self.source_table}.type_of_idc_description IS NOT NULL
                             AND ucase({self.source_table}.type_of_idc_description) <> 'NAN'
                           THEN {self.source_table}.type_of_idc_description
                         WHEN {self.source_table}.idv_type = 'B'
                           THEN 'INDEFINITE DELIVERY CONTRACT'
                         ELSE {self.source_table}.idv_type_description
                    END AS type_description
                    """,
            ]
        )
        for col in DAP_TO_NORMALIZED_COLUMN_INFO:
            select_cols.append(self.handle_column(col))
        return select_cols
