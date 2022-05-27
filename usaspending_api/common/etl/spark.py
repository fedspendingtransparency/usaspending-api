from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import get_jvm_logger

MAX_PARTITIONS = CONFIG.SPARK_MAX_PARTITIONS


def extract_db_data_frame(
    spark: SparkSession,
    conn_props: dict,
    jdbc_url: str,
    partition_rows: int,
    min_max_sql: str,
    table: str,
    partitioning_col: str,
    is_numeric_partitioning_col: bool = True,
    is_date_partitioning_col: bool = False,
    custom_schema: StructType = None,
) -> DataFrame:
    logger = get_jvm_logger(spark)

    logger.info(f"Getting partition bounds using SQL:\n{min_max_sql}")

    data_df = None

    logger.info(
        f"Running extract for table {table} "
        f"with is_numeric_partitioning_col = {is_numeric_partitioning_col} and "
        f"is_date_partitioning_col = {is_date_partitioning_col}"
    )
    if is_numeric_partitioning_col and is_date_partitioning_col:
        raise ValueError("Partitioning col cannot be both numeric and date. Pick one.")

    # Get the bounds of the data we are extracting, so we can let spark partition it
    min_max_df = spark.read.jdbc(url=jdbc_url, table=min_max_sql, properties=conn_props)
    if is_date_partitioning_col:
        # Ensure it is a date (e.g. if date in string format, convert to date)
        min_max_df = min_max_df.withColumn(min_max_df.columns[0], to_date(min_max_df[0])).withColumn(
            min_max_df.columns[1], to_date(min_max_df[1])
        )
    min_max = min_max_df.first()
    min_val = min_max[0]
    max_val = min_max[1]
    count = min_max[2]

    if is_numeric_partitioning_col:
        logger.info(f"Deriving partitions from numeric ranges across column: {partitioning_col}")
        # Take count as partition if using a spotty range, and count of rows is less than range of IDs
        partitions = int(min((int(max_val) - int(min_val)), int(count)) / (partition_rows + 1))
        logger.info(f"Derived {partitions} partitions from numeric ranges across column: {partitioning_col}")
        if partitions > MAX_PARTITIONS:
            fail_msg = (
                f"Aborting job run because {partitions} partitions "
                f"is greater than the max allowed by this job ({MAX_PARTITIONS})"
            )
            logger.fatal(fail_msg)
            raise RuntimeError(fail_msg)

        logger.info(f"{partitions} partitions to extract at approximately {partition_rows} rows each.")

        data_df = spark.read.options(customSchema=custom_schema).jdbc(
            url=jdbc_url,
            table=table,
            column=partitioning_col,
            lowerBound=min_val,
            upperBound=max_val,
            numPartitions=partitions,
            properties=conn_props,
        )
    elif is_date_partitioning_col:
        logger.info(f"Deriving partitions from dates in column: {partitioning_col}")
        # Assume we want a partition per distinct date, and cover every date in the range from min to max, inclusive
        # But if the fill-factor is not > 60% in that range, i.e. if the distinct count of dates in our data is not
        # 3/5ths or more of the total dates in that range, use the distinct date values from the data set -- but ONLY
        # if that distinct count is less than MAX_PARTITIONS
        date_delta = max_val - min_val
        partitions = date_delta.days + 1
        if (count / partitions) < 0.6 or True:  # Forcing this path, see comment in else below
            logger.info(
                f"Partitioning by date in col {partitioning_col} would yield {partitions} but only {count} "
                f"distinct dates in the dataset. This partition range is too sparse. Going to query the "
                f"distinct dates and use as partitions if less than MAX_PARTITIONS ({MAX_PARTITIONS})"
            )
            if count > MAX_PARTITIONS:
                fail_msg = (
                    f"Aborting job run because {partitions} partitions "
                    f"is greater than the max allowed by this job ({MAX_PARTITIONS})"
                )
                logger.fatal(fail_msg)
                raise RuntimeError(fail_msg)
            else:
                date_df = spark.read.jdbc(
                    url=jdbc_url,
                    table=f"(select distinct {partitioning_col} from {table}) distinct_dates",
                    properties=conn_props,
                )
                partition_sql_predicates = [f"{partitioning_col} = '{str(row[0])}'" for row in date_df.collect()]
                logger.info(
                    f"Built {len(partition_sql_predicates)} SQL partition predicates "
                    f"to yield data partitions, based on distinct values of {partitioning_col} "
                )

                data_df = spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    predicates=partition_sql_predicates,
                    properties=conn_props,
                )
        else:
            # Getting a partition for each date in the range of dates from min to max, inclusive
            logger.info(
                f"Derived {partitions} partitions from min ({min_val}) to max ({max_val}) date range "
                f"across column: {partitioning_col}, with data for {(count / partitions):.1%} of those dates"
            )
            if partitions > MAX_PARTITIONS:
                fail_msg = (
                    f"Aborting job run because {partitions} partitions "
                    f"is greater than the max allowed by this job ({MAX_PARTITIONS})"
                )
                logger.fatal(fail_msg)
                raise RuntimeError(fail_msg)
            else:
                # NOTE: Have to use integer (really a Long) representation of the Date, since that is what the Scala
                # ... implementation is expecting: https://github.com/apache/spark/blob/c561ee686551690bee689f37ae5bbd75119994d6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L192-L207
                # TODO: THIS DOES NOT SEEM TO WORK WITH DATES for lowerBound and upperBound. Forcing use of predicates
                raise NotImplementedError("Cannot read JDBC partitions with date lower/upper bound")

                data_df = spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    column=partitioning_col,
                    lowerBound=min_val,
                    upperBound=max_val,
                    numPartitions=partitions,
                    properties=conn_props,
                )
    else:
        logger.info(f"Deriving partitions from dates in column: {partitioning_col}")
        partitions = int(count / (partition_rows + 1))
        logger.info(
            f"Derived {partitions} partitions from {count} distinct non-numeric (text) "
            f"values in column: {partitioning_col}."
        )
        if partitions > MAX_PARTITIONS:
            fail_msg = (
                f"Aborting job run because {partitions} partitions "
                f"is greater than the max allowed by this job ({MAX_PARTITIONS})"
            )
            logger.fatal(fail_msg)
            raise RuntimeError(fail_msg)

        # SQL usable in Postgres to get a distinct 32-bit int from an md5 hash of text
        pg_int_from_hash = f"('x'||substr(md5({partitioning_col}::text),1,8))::bit(32)::int"
        # int could be signed. This workaround SQL gets unsigned modulus from the hash int
        non_neg_modulo = f"mod({partitions} + mod({pg_int_from_hash}, {partitions}), {partitions})"
        partition_sql_predicates = [f"{non_neg_modulo} = {p}" for p in range(0, partitions)]

        logger.info(f"{partitions} partitions to extract by predicates at approximately {partition_rows} rows each.")

        data_df = spark.read.jdbc(
            url=jdbc_url,
            table=table,
            predicates=partition_sql_predicates,
            properties=conn_props,
        )

    return data_df


def get_partition_bounds_sql(
    table_name: str,
    partitioning_col_name: str,
    partitioning_col_alias: str,
    is_partitioning_col_unique: bool = True,
    core_sql: str = None,
    row_limit: int = None,
) -> str:
    if not row_limit and is_partitioning_col_unique:
        sql = f"""
        (
            select
                min({table_name}.{partitioning_col_alias}),
                max({table_name}.{partitioning_col_alias}),
                count({table_name}.{partitioning_col_alias})
            {core_sql if core_sql else "from " + table_name}
        ) min_max
        """
    else:
        sql = f"""
        (
            select
                min(limited.{partitioning_col_alias}),
                max(limited.{partitioning_col_alias}),
                count(limited.{partitioning_col_alias})
            from (
                -- distinct allows for creating partitions (row-chunks) on non-primary key columns
                select distinct {table_name}.{partitioning_col_name} as {partitioning_col_alias}
                {core_sql if core_sql else "from " + table_name}
                where {table_name}.{partitioning_col_name} is not null
                limit {row_limit if row_limit else "NULL"}
            ) limited
        ) min_max
        """
    return sql


def load_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    delta_table_name: str,
    overwrite: bool = False,
) -> None:
    """
    Write DataFrame data to a table in Delta format.
    Args:
        spark: the SparkSession
        source_df: DataFrame with data to write
        delta_table_name: table to write into. Currently this function requires the table to already exist.
        overwrite: If True, will replace all existing data with that of the DataFrame, while append will add new data.
            If left False (the default), the DataFrame data will be appended to existing data.
    Returns: None
    """
    logger = get_jvm_logger(spark)
    logger.info(f"LOAD (START): Loading data into Delta table {delta_table_name}")
    # NOTE: Best to (only?) use .saveAsTable(name=<delta_table>) rather than .insertInto(tableName=<delta_table>)
    # ... The insertInto does not seem to align/merge columns from DataFrame to table columns (defaults to column order)
    save_mode = "overwrite" if overwrite else "append"
    source_df.write.format(source="delta").mode(saveMode=save_mode).saveAsTable(name=delta_table_name)
    logger.info(f"LOAD (FINISH): Loaded data into Delta table {delta_table_name}")


def load_es_index(
    spark: SparkSession, source_df: DataFrame, base_config: dict, index_name: str, routing: str, doc_id: str
) -> None:  # pragma: no cover -- will be used and tested eventually
    index_config = base_config.copy()
    index_config["es.resource.write"] = index_name
    index_config["es.mapping.routing"] = routing
    index_config["es.mapping.id"] = doc_id

    # JVM-based Python utility function to convert a python dictionary to a scala Map[String, String]
    jvm_es_config_map = source_df._jmap(index_config)

    # Conversion of Python DataFrame to JVM DataFrame
    jvm_data_df = source_df._jdf

    # Call the elasticsearch-hadoop method to write the DF to ES via the _jvm conduit on the SparkContext
    spark.sparkContext._jvm.org.elasticsearch.spark.sql.EsSparkSQL.saveToEs(jvm_data_df, jvm_es_config_map)


def merge_delta_table(spark: SparkSession, source_df: DataFrame, delta_table_name: str, merge_column: str):
    source_df.create_or_replace_temporary_view("temp_table")

    spark.sql(
        rf"""
        MERGE INTO {delta_table_name} USING temp_table
            ON {delta_table_name}.{merge_column} = temp_table.{merge_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )
