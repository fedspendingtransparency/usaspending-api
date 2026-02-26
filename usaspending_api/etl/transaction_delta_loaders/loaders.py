import logging
from abc import ABC
from datetime import datetime, timezone
from typing import Callable, Literal

from delta import DeltaTable
from pyspark.sql import Column, DataFrame, functions as sf, SparkSession, Window
from pyspark.sql.types import ArrayType, StringType


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
from usaspending_api.etl.transaction_delta_loaders.utils import parse_date_column

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
    last_etl_load_date: datetime

    def __init__(self, spark, etl_level: Literal["fabs", "fpds", "normalized"], spark_s3_bucket: str) -> None:
        self.etl_level = etl_level
        self.last_etl_load_date = get_last_load_date(f"transaction_{self.etl_level}")
        self.spark_s3_bucket: spark_s3_bucket
        self.spark = spark

    def load_transactions(self) -> None:
        logger.info(f"LOADING TRANSACTIONS -- level: {self.etl_level}, last load date: {self.last_etl_load_date}")
        if not self.spark._jsparkSession.catalog().tableExists(f"int.transaction_{self.etl_level}"):
            raise Exception(f"Table: int.transaction_{self.etl_level} does not exist.")
        logger.info(f"Running UPSERT SQL for transaction_{self.etl_level} ETL")
        self.transaction_merge()
        next_last_load = get_earliest_load_date(
            ("source_procurement_transaction", "source_assistance_transaction"), datetime.utcfromtimestamp(0)
        )
        update_last_load_date(f"transaction_{self.etl_level}", next_last_load)

    def handle_column(self, col: TransactionColumn, is_result_aliased=True) -> Column:
        if col.handling == "cast":
            retval = sf.col(f"{self.source_table}.{col.source}").cast(col.delta_type)
        elif col.handling == "literal":
            # Use col.source directly as the value
            retval = sf.lit(col.source).cast(col.delta_type)
        elif col.handling == "parse_string_datetime_to_date":
            # These are string fields that actually hold DATES/TIMESTAMPS and need to be cast as dates.
            # However, they may not be properly parsed when calling CAST(... AS DATE).
            retval = parse_date_column(col.source, table=self.source_table, is_casted_to_date=True)
        elif col.handling == "string_datetime_remove_timestamp":
            # These are string fields that actually hold DATES/TIMESTAMPS, but need the non-DATE part discarded,
            # even though they remain as strings
            retval = parse_date_column(col.source, table=self.source_table, is_casted_to_date=False)
        elif col.delta_type.upper() == "STRING":
            # Capitalize and remove leading & trailing whitespace from all string values
            retval = sf.upper(sf.trim(sf.col(f"{self.source_table}.{col.source}")))
        elif col.delta_type.upper() == "BOOLEAN" and not col.handling == "leave_null":
            # Unless specified, convert any nulls to false for boolean columns
            retval = sf.coalesce(sf.col(f"{self.source_table}.{col.source}"), sf.lit(False))
        else:
            retval = sf.col(f"{self.source_table}.{col.source}")

        # Handle scalar transformations if the column requires it
        if col.scalar_transformation is not None:
            retval = col.scalar_transformation(retval)

        retval = retval.alias(col.dest_name) if is_result_aliased else retval
        return retval

    @property
    def select_columns(self) -> list[Column]:
        return [sf.lit(None).cast("LONG").alias("transaction_id")] + [
            self.handle_column(col) for col in self.col_info if col.dest_name != "transaction_id"
        ]

    def to_insert_df(self) -> DataFrame:
        window_spec = Window.partitionBy(self.id_col)
        return (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingTimestamp", self.last_etl_load_date.strftime("%Y-%m-%d %H:%M:%S"))
            .table(self.source_table)
            .withColumn("latest_version", sf.max("_commit_version").over(window_spec))
            .filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
                & (sf.col("_commit_version") == sf.col("latest_version"))
            )
            .select(self.select_columns)
        )

    def to_delete_df(self, id_col) -> DataFrame:
        version_window = Window.partitionBy(id_col, "hash", "_commit_version")
        transaction_window = Window.partitionBy(id_col, "hash")
        return (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingTimestamp", self.last_etl_load_date.strftime("%Y-%m-%d %H:%M:%S"))
            .table(self.source_table)
            .withColumn("latest_version", sf.max(sf.col("_commit_version")).over(transaction_window))
            .withColumn("has_insert", sf.max(sf.col("_change_type") == "insert").over(version_window))
            .filter(
                (sf.col("_change_type") == sf.lit("delete"))
                & (sf.col("_commit_version") == sf.col("latest_version"))
                & ~sf.col("has_insert")
            )
            .select(id_col, "hash", "action_year", "action_month")
        )

    def transaction_merge(self) -> None:
        source = self.to_insert_df().alias("s")
        logger.info(f"number of rows: {source.count()}")
        target = DeltaTable.forName(self.spark, f"int.transaction_{self.etl_level}").alias("t")
        id_condition = f"t.{self.id_col} == s.{self.id_col}"
        hash_condition = "t.hash == s.hash"
        partition_pruning_conditions = "t.action_year == s.action_year AND t.action_month == s.action_month"
        (
            target.merge(source, " AND ".join([id_condition, hash_condition, partition_pruning_conditions]))
            .whenNotMatchedInsert(
                values={
                    col.dest_name: sf.col(f"s.{col.dest_name}")
                    for col in self.col_info
                    if col.dest_name != "transaction_id"
                },
            )
            .execute()
        )
        (
            target.merge(
                self.to_delete_df(self.id_col).alias("s"),
                " AND ".join([id_condition, hash_condition, partition_pruning_conditions]),
            )
            .whenMatchedDelete()
            .execute()
        )


class FPDSDeltaTransactionLoader(AbstractDeltaTransactionLoader):

    def __init__(self, spark: SparkSession, spark_s3_bucket: str) -> None:
        super().__init__(spark=spark, etl_level="fpds", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "detached_award_proc_unique"
        self.source_table = "raw.detached_award_procurement"
        self.col_info = TRANSACTION_FPDS_COLUMN_INFO


class FABSDeltaTransactionLoader(AbstractDeltaTransactionLoader):

    def __init__(self, spark: SparkSession, spark_s3_bucket: str) -> None:
        super().__init__(spark=spark, etl_level="fabs", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "afa_generated_unique"
        self.source_table = "raw.published_fabs"
        self.col_info = TRANSACTION_FABS_COLUMN_INFO


class NormalizedMixin:

    spark: SparkSession
    handle_column: Callable
    to_delete_df: Callable
    source_table: str
    id_col: str
    source_id_col: str
    etl_level: str
    last_etl_load_date: datetime
    select_columns: list[str]
    to_normalized_col_info: list[TransactionColumn]
    normalization_type: Literal["fabs", "fpds"]

    def to_insert_df(self) -> DataFrame:
        funding_subtier_agency = self.spark.table("global_temp.subtier_agency").alias("funding_subtier_agency")
        funding_agency = self.spark.table("global_temp.agency").alias("funding_agency")
        awarding_subtier_agency = (
            self.spark.table("global_temp.subtier_agency")
            .withColumn("awarding_subtier_agency_id", sf.col("subtier_agency_id"))
            .alias("awarding_subtier_agency")
        )
        awarding_agency = self.spark.table("global_temp.agency").alias("awarding_agency")
        window_spec = Window.partitionBy(self.source_id_col)
        df = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingTimestamp", self.last_etl_load_date.strftime("%Y-%m-%d %H:%M:%S"))
            .table(self.source_table)
            .withColumn("latest_version", sf.max("_commit_version").over(window_spec))
            .filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
                & (sf.col("_commit_version") == sf.col("latest_version"))
            )
        )
        result = (
            df.join(
                funding_subtier_agency,
                funding_subtier_agency.subtier_code == df.funding_sub_tier_agency_co,
                how="leftouter",
            )
            .join(
                funding_agency,
                funding_agency.subtier_agency_id == funding_subtier_agency.subtier_agency_id,
                how="leftouter",
            )
            .join(
                awarding_subtier_agency,
                awarding_subtier_agency.subtier_code == df.awarding_sub_tier_agency_c,
                how="leftouter",
            )
            .join(
                awarding_agency,
                awarding_agency.subtier_agency_id == awarding_subtier_agency.awarding_subtier_agency_id,
                how="leftouter",
            )
            .select(self.select_columns)
        )
        return result

    def transaction_merge(self) -> None:
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
        insert_col_names = [col_name for col_name in TRANSACTION_NORMALIZED_COLUMNS if col_name not in special_columns]
        insert_col_names.extend(special_columns)

        # On insert, all values except for create_date and update_date will come from the subquery
        insert_values = [sf.col(col) for col in insert_col_names[:-2]]
        insert_values.extend([sf.lit(f"{load_datetime.isoformat(sep=' ')}")] * 2)

        target = DeltaTable.forName(self.spark, "int.transaction_normalized").alias("t")
        id_condition = "t.transaction_unique_id = s.transaction_unique_id"
        hash_condition = "t.hash == s.hash"
        type_partition_condition = f"{'NOT' if self.normalization_type == 'fabs' else ''} t.is_fpds"
        partition_pruning_conditions = "t.action_year == s.action_year AND t.action_month == s.action_month"
        (
            target.merge(
                self.to_insert_df().alias("s"),
                " AND ".join([id_condition, hash_condition, type_partition_condition, partition_pruning_conditions]),
            )
            .whenNotMatchedInsert(values=dict(zip(insert_col_names, insert_values)))
            .execute()
        )
        delete_id_condition = f"t.transaction_unique_id = s.{self.source_id_col}"
        (
            target.merge(
                self.to_delete_df(self.source_id_col).alias("s"),
                " AND ".join([delete_id_condition, hash_condition, partition_pruning_conditions]),
            )
            .whenMatchedDelete()
            .execute()
        )

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
        w = Window.orderBy(needs_ids.unique_award_key)
        with_ids = needs_ids.withColumn("award_id", (max_id + sf.row_number().over(w)).cast("LONG")).alias("s")
        (
            target.merge(with_ids, f"t.unique_award_key = s.unique_award_key")
            .whenMatchedUpdate(set={"t.award_id": "s.award_id"})
            .execute()
        )

    def load_transactions(self) -> None:
        super().load_transactions()
        self.populate_award_ids()
        self.populate_transaction_normalized_ids()
        self.link_transactions_to_normalized()


class FABSNormalizedDeltaTransactionLoader(NormalizedMixin, AbstractDeltaTransactionLoader):

    def __init__(self, spark: SparkSession, spark_s3_bucket: str) -> None:
        super().__init__(spark=spark, etl_level="normalized", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "transaction_unique_id"
        self.source_id_col = "afa_generated_unique"
        self.source_table = "raw.published_fabs"
        self.to_normalized_col_info = FABS_TO_NORMALIZED_COLUMN_INFO
        self.normalization_type = "fabs"

    @property
    def select_columns(self) -> list[str]:
        action_date_col = next(
            filter(lambda c: c.dest_name == "action_date" and c.source == "action_date", FABS_TO_NORMALIZED_COLUMN_INFO)
        )
        parse_action_date_snippet = self.handle_column(action_date_col, is_result_aliased=False)
        select_cols = [
            sf.lit(None).cast("LONG").alias("id"),
            sf.lit(None).cast("LONG").alias("award_id"),
            sf.col("awarding_agency.id").alias("awarding_agency_id"),
            sf.when(sf.month(parse_action_date_snippet) > sf.lit(9), sf.year(parse_action_date_snippet) + sf.lit(1))
            .otherwise(sf.year(parse_action_date_snippet))
            .alias("fiscal_year"),
            sf.col("funding_agency.id").alias("funding_agency_id"),
        ]
        get_business_categories_fabs_udf = sf.udf(
            lambda x: get_business_categories_fabs(x),
            ArrayType(StringType()),
        )
        select_cols = select_cols + [
            get_business_categories_fabs_udf(sf.col(f"{self.source_table}.business_types")).alias(
                "business_categories"
            ),
            sf.expr(f"CAST({self.source_table}.total_funding_amount AS NUMERIC(23, 2)) AS funding_amount"),
        ]

        for col in FABS_TO_NORMALIZED_COLUMN_INFO:
            select_cols.append(self.handle_column(col))
        return select_cols


class FPDSNormalizedDeltaTransactionLoader(NormalizedMixin, AbstractDeltaTransactionLoader):

    def __init__(self, spark, spark_s3_bucket: str) -> None:
        super().__init__(spark=spark, etl_level="normalized", spark_s3_bucket=spark_s3_bucket)
        self.id_col = "transaction_unique_id"
        self.source_id_col = "detached_award_proc_unique"
        self.source_table = "raw.detached_award_procurement"
        self.to_normalized_col_info = DAP_TO_NORMALIZED_COLUMN_INFO
        self.normalization_type = "fpds"

    @property
    def select_columns(self) -> list[str]:
        action_date_col = next(
            filter(lambda c: c.dest_name == "action_date" and c.source == "action_date", DAP_TO_NORMALIZED_COLUMN_INFO)
        )
        parse_action_date_snippet = self.handle_column(action_date_col, is_result_aliased=False)
        select_cols = [
            sf.lit(None).cast("LONG").alias("id"),
            sf.lit(None).cast("LONG").alias("award_id"),
            sf.col("awarding_agency.id").alias("awarding_agency_id"),
            sf.when(sf.month(parse_action_date_snippet) > sf.lit(9), sf.year(parse_action_date_snippet) + sf.lit(1))
            .otherwise(sf.year(parse_action_date_snippet))
            .alias("fiscal_year"),
            sf.col("funding_agency.id").alias("funding_agency_id"),
        ]
        fpds_business_category_columns = [
            sf.col(col) for col in fpds_boolean_columns + ["contracting_officers_deter", "domestic_or_foreign_entity"]
        ]
        get_business_categories_fpds_udf = sf.udf(lambda x: get_business_categories_fpds(x), ArrayType(StringType()))
        select_cols.extend(
            [
                get_business_categories_fpds_udf(sf.struct(*fpds_business_category_columns)).alias(
                    "business_categories"
                ),
                # type
                sf.expr(
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
                    """
                ),
                # type_description
                sf.expr(
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
                    """
                ),
            ]
        )
        for col in DAP_TO_NORMALIZED_COLUMN_INFO:
            select_cols.append(self.handle_column(col))
        return select_cols
