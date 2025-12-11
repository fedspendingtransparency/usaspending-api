import copy
import logging
import re
from contextlib import contextmanager
from datetime import datetime, timezone

from django.core.management import BaseCommand, call_command
from django.db import connection
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.utils import AnalysisException

from usaspending_api.awards.delta_models.awards import AWARDS_COLUMNS
from usaspending_api.broker.helpers.build_business_categories_boolean_dict import fpds_boolean_columns
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import (
    get_earliest_load_date,
    get_last_load_date,
    update_last_load_date,
)
from usaspending_api.common.data_classes import TransactionColumn
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.config import CONFIG
from usaspending_api.transactions.delta_models.transaction_fabs import (
    FABS_TO_NORMALIZED_COLUMN_INFO,
    TRANSACTION_FABS_COLUMN_INFO,
    TRANSACTION_FABS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    DAP_TO_NORMALIZED_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables
        specified via the "etl_level" argument. Each "etl_level" uses an exclusive value for "last_load_date" from the
        "external_data_load_date" table in Postgres to determine the subset of transactions to load. For a full
        pipeline run the "award_id_lookup" and "transaction_id_lookup" levels should be run first in order to populate the
        lookup tables. These lookup tables are used to keep track of PK values across the different silver tables.

        *****NOTE*****: Before running this command for the first time on a usual basis, it should be run with the
            "etl_level" set to "initial_run" to set up the needed lookup tables and populate the needed sequences and
            "last_load_date" values for the lookup tables.
    """

    last_etl_date: str
    spark_s3_bucket: str
    spark: SparkSession
    # See comments in delete_records_sql, transaction_id_lookup ETL level, for more info about logic in the
    # query below.

    def add_arguments(self, parser):
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 for creating the tables.",
        )

    def handle(self, *args, **options):
        with self.prepare_spark():
            self.spark_s3_bucket = options["spark_s3_bucket"]

            # Capture earliest last load date of the source tables to update the "last_load_date" after completion
            next_last_load = get_earliest_load_date(
                ("source_procurement_transaction", "source_assistance_transaction"), datetime.utcfromtimestamp(0)
            )

            # Do this check now to avoid uncaught errors later when running queries
            # Use 'int' because that is what will be targeted for deletes/updates/etc.
            table_exists = self.spark._jsparkSession.catalog().tableExists("int.transaction_normalized")
            if not table_exists:
                raise Exception(f"Table: int.transaction_normalized does not exist.")

            logger.info(f"Running delete SQL for 'transaction_normalized' ETL")
            self.spark.sql(self.delete_records_sql())

            last_etl_date = None  # get_last_load_date(self.etl_level)
            if last_etl_date is None:
                # Table has not been loaded yet.  To avoid checking for None in all the locations where
                # last_etl_date is used, set it to a long time ago.
                last_etl_date = datetime.utcfromtimestamp(0)
            self.last_etl_date = str(last_etl_date)

            logger.info(f"Running UPSERT SQL for 'transaction_normalized' ETL")
            create_ref_temp_views(self.spark)
            self.spark.sql(self.transaction_normalized_merge_into_sql("fabs"))
            self.spark.sql(self.transaction_normalized_merge_into_sql("fpds"))

            update_last_load_date("transaction_normalized", next_last_load)

    @contextmanager
    def prepare_spark(self):
        extra_conf = {
            # Config for additional packages needed
            # "spark.jars.packages": "org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hive_2.12:3.2.1",
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
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

    @staticmethod
    def delete_records_sql():
        subquery = f"""
            SELECT tn.id AS id_to_remove
            FROM int.transaction_normalized tn
            LEFT JOIN raw.detached_award_procurement dap ON (
                tn.is_fpds = TRUE AND tn.transaction_unique_id = UPPER(dap.detached_award_proc_unique) 
            )
            LEFT JOIN raw.published_fabs pf ON (
                tn.is_fpds = FALSE AND tn.transaction_unique_id = UPPER(pf.afa_generated_unique) 
            )
            WHERE 
                (
                    tn.is_fpds = TRUE
                    AND tn.transaction_unique_id IS NOT NULL 
                    AND dap.detached_award_proc_unique IS NULL
                ) OR 
                (
                    tn.is_fpds = FALSE
                    AND tn.transaction_unique_id IS NOT NULL 
                    AND pf.afa_generated_unique IS NULL
                )
        """

        sql = f"""
            MERGE INTO int.transaction_normalized
            USING (
                {subquery}
            ) AS deleted_records
            ON transaction_normalized.id = deleted_records.id_to_remove
            WHEN MATCHED
            THEN DELETE
        """

        return sql

    def source_subquery_sql(self, transaction_type=None):
        def build_date_format_sql(col: TransactionColumn, is_casted_to_date: bool = True) -> str:
            """Builder function to wrap a column in date-parsing logic.

            It will either parse it in mmddYYYY format with - or / as a required separator, or in YYYYmmdd format
            with or without either of - or / as a separator.
            Args:
                is_casted_to_date (bool): if true, the parsed result will be cast to DATE to provide a DATE datatype,
                    otherwise it remains a STRING in YYYY-mm-dd format
            """
            # Each of these regexps allows for an optional timestamp portion, separated from the date by some character,
            #   and the timestamp allows for an optional UTC offset.  In any case, the timestamp is ignored, though.
            regexp_mmddYYYY = (
                r"(\\d{2})(?<sep>[-/])(\\d{2})(\\k<sep>)(\\d{4})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"
            )
            regexp_YYYYmmdd = (
                r"(\\d{4})(?<sep>[-/]?)(\\d{2})(\\k<sep>)(\\d{2})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"
            )

            mmddYYYY_fmt = f"""
                (regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 5)
                || '-' ||
                regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 1)
                || '-' ||
                regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 3))
            """
            YYYYmmdd_fmt = f"""
                (regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 1)
                || '-' ||
                regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 3)
                || '-' ||
                regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 5))
            """

            if is_casted_to_date:
                mmddYYYY_fmt = f"""CAST({mmddYYYY_fmt}
                            AS DATE)
                """
                YYYYmmdd_fmt = f"""CAST({YYYYmmdd_fmt}
                            AS DATE)
                """

            sql_snippet = f"""
                CASE WHEN regexp({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}')
                          THEN {mmddYYYY_fmt}
                     ELSE {YYYYmmdd_fmt}
                END
            """

            return sql_snippet

        def handle_column(col: TransactionColumn, bronze_table_name, is_result_aliased=True):
            """
            Args:
                is_result_aliased (bool) if true, aliases the parsing result with the given ``col``'s ``dest_name``
            """
            if col.handling == "cast":
                retval = f"CAST({bronze_table_name}.{col.source} AS {col.delta_type})"
            elif col.handling == "literal":
                # Use col.source directly as the value
                retval = f"{col.source}"
            elif col.handling == "parse_string_datetime_to_date":
                # These are string fields that actually hold DATES/TIMESTAMPS and need to be cast as dates.
                # However, they may not be properly parsed when calling CAST(... AS DATE).
                retval = build_date_format_sql(col, is_casted_to_date=True)
            elif col.handling == "string_datetime_remove_timestamp":
                # These are string fields that actually hold DATES/TIMESTAMPS, but need the non-DATE part discarded,
                # even though they remain as strings
                retval = build_date_format_sql(col, is_casted_to_date=False)
            elif col.delta_type.upper() == "STRING":
                # Capitalize and remove leading & trailing whitespace from all string values
                retval = f"ucase(trim({bronze_table_name}.{col.source}))"
            elif col.delta_type.upper() == "BOOLEAN" and not col.handling == "leave_null":
                # Unless specified, convert any nulls to false for boolean columns
                retval = f"COALESCE({bronze_table_name}.{col.source}, FALSE)"
            else:
                retval = f"{bronze_table_name}.{col.source}"

            # Handle scalar transformations if the column requires it
            if col.scalar_transformation is not None:
                retval = col.scalar_transformation.format(input=retval)

            retval = f"{retval}{' AS ' + col.dest_name if is_result_aliased else ''}"
            return retval

        def select_columns_transaction_normalized(bronze_table_name):
            action_date_col = next(
                filter(
                    lambda c: c.dest_name == "action_date" and c.source == "action_date",
                    FABS_TO_NORMALIZED_COLUMN_INFO if transaction_type == "fabs" else DAP_TO_NORMALIZED_COLUMN_INFO,
                )
            )
            parse_action_date_sql_snippet = handle_column(action_date_col, bronze_table_name, is_result_aliased=False)
            select_cols = [
                # TODO remove the award_id and use the unique_award_key create the relation between transactions and awards
                "1 as award_id",
                "awarding_agency.id AS awarding_agency_id",
                f"""CASE WHEN month({parse_action_date_sql_snippet}) > 9
                             THEN year({parse_action_date_sql_snippet}) + 1
                         ELSE year({parse_action_date_sql_snippet})
                    END AS fiscal_year""",
                "funding_agency.id AS funding_agency_id",
            ]

            if transaction_type == "fabs":
                select_cols.extend(
                    [
                        # business_categories
                        f"get_business_categories_fabs({bronze_table_name}.business_types) AS business_categories",
                        # funding_amount
                        # In theory, this should be equal to
                        #   CAST(COALESCE({bronze_table_name}.federal_action_obligation, 0)
                        #        + COALESCE({bronze_table_name}.non_federal_funding_amount, 0)
                        #        AS NUMERIC(23, 2))
                        #   However, for some historical records, this isn't true.
                        f"""
                        CAST({bronze_table_name}.total_funding_amount AS NUMERIC(23, 2)) AS funding_amount
                        """,
                    ]
                )
                map_col_info = copy.copy(FABS_TO_NORMALIZED_COLUMN_INFO)
            else:
                fpds_business_category_columns = copy.copy(fpds_boolean_columns)
                # Add a couple of non-boolean columns that are needed in the business category logic
                fpds_business_category_columns.extend(["contracting_officers_deter", "domestic_or_foreign_entity"])
                named_struct_text = ", ".join(
                    [f"'{col}', {bronze_table_name}.{col}" for col in fpds_business_category_columns]
                )

                select_cols.extend(
                    [
                        # business_categories
                        f"get_business_categories_fpds(named_struct({named_struct_text})) AS business_categories",
                        # type
                        f"""
                        CASE WHEN {bronze_table_name}.pulled_from <> 'IDV' THEN {bronze_table_name}.contract_award_type
                             WHEN {bronze_table_name}.idv_type = 'B' AND {bronze_table_name}.type_of_idc IS NOT NULL
                               THEN 'IDV_B_' || {bronze_table_name}.type_of_idc
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description = 'INDEFINITE DELIVERY / REQUIREMENTS'
                               THEN 'IDV_B_A'
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description =
                                     'INDEFINITE DELIVERY / INDEFINITE QUANTITY'
                               THEN 'IDV_B_B'
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description =
                                     'INDEFINITE DELIVERY / DEFINITE QUANTITY'
                               THEN 'IDV_B_C'
                             ELSE 'IDV_' || {bronze_table_name}.idv_type
                        END AS type
                        """,
                        # type_description
                        f"""
                        CASE WHEN {bronze_table_name}.pulled_from <> 'IDV'
                               THEN {bronze_table_name}.contract_award_type_desc
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description IS NOT NULL
                                 AND ucase({bronze_table_name}.type_of_idc_description) <> 'NAN'
                               THEN {bronze_table_name}.type_of_idc_description
                             WHEN {bronze_table_name}.idv_type = 'B'
                               THEN 'INDEFINITE DELIVERY CONTRACT'
                             ELSE {bronze_table_name}.idv_type_description
                        END AS type_description
                        """,
                    ]
                )
                map_col_info = copy.copy(DAP_TO_NORMALIZED_COLUMN_INFO)

            for col in map_col_info:
                select_cols.append(handle_column(col, bronze_table_name))

            return select_cols

        if transaction_type == "fabs":
            bronze_table_name = "raw.published_fabs"
            unique_id = "afa_generated_unique"
        elif transaction_type == "fpds":
            bronze_table_name = "raw.detached_award_procurement"
            unique_id = "detached_award_proc_unique"
        else:
            raise ValueError(
                f"Invalid value for 'transaction_type': {transaction_type}; must select either: 'fabs' or 'fpds'"
            )

        id_col_name = "id"
        select_columns = select_columns_transaction_normalized(bronze_table_name)
        additional_joins = f"""            
            LEFT OUTER JOIN global_temp.subtier_agency AS funding_subtier_agency ON (
                funding_subtier_agency.subtier_code = {bronze_table_name}.funding_sub_tier_agency_co
            )
            LEFT OUTER JOIN global_temp.agency AS funding_agency ON (
                funding_agency.subtier_agency_id = funding_subtier_agency.subtier_agency_id
            )
            LEFT OUTER JOIN global_temp.subtier_agency AS awarding_subtier_agency ON (
                awarding_subtier_agency.subtier_code = {bronze_table_name}.awarding_sub_tier_agency_c
            )
            LEFT OUTER JOIN global_temp.agency AS awarding_agency ON (
                awarding_agency.subtier_agency_id = awarding_subtier_agency.subtier_agency_id
            )
        """

        # Since the select columns may have complicated logic, put them on separate lines for debugging.
        # However, strings inside {} expressions in f-strings can't contain backslashes, so will join them first
        # before inserting into overall sql statement.
        select_columns_str = ",\n    ".join(select_columns)
        sql = f"""
            SELECT
                -- TODO We should remove the id columns and use the unique_id column as the primary key.
                row_number() over (order by {unique_id}) AS {id_col_name},
                {select_columns_str}
            FROM {bronze_table_name}            
            {additional_joins}
            WHERE {bronze_table_name}.updated_at >= '{self.last_etl_date}'
        """

        return sql

    def transaction_normalized_merge_into_sql(self, transaction_type):
        if transaction_type != "fabs" and transaction_type != "fpds":
            raise ValueError(
                f"Invalid value for 'transaction_type': {transaction_type}. Must select either: 'fabs' or 'fpds'"
            )

        load_datetime = datetime.now(timezone.utc)
        special_columns = ["create_date", "update_date"]

        # On set, create_date will not be changed and update_date will be set below.  All other column
        # values will come from the subquery.
        set_cols = [
            f"int.transaction_normalized.{col_name} = source_subquery.{col_name}"
            for col_name in TRANSACTION_NORMALIZED_COLUMNS
            if col_name not in (special_columns + ["id"])
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
                {self.source_subquery_sql(transaction_type)}
            ) AS source_subquery
            ON transaction_normalized.transaction_unique_id = source_subquery.transaction_unique_id
            WHEN MATCHED
                THEN UPDATE SET
                {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({insert_col_names})
                    VALUES ({insert_values})
        """

        return sql
