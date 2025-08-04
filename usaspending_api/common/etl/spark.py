"""
Spark utility functions that could be used as stages or steps of an ETL job (aka "data pipeline")

NOTE: This is distinguished from the usaspending_api.common.helpers.spark_helpers module, which holds mostly boilerplate
functions for setup and configuration of the spark environment
"""

import logging
import time
from collections import namedtuple
from itertools import chain
from typing import List

from py4j.protocol import Py4JError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, concat_ws, expr, lit, regexp_replace, to_date, transform, when
from pyspark.sql.types import ArrayType, DecimalType, StringType, StructType

from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.common.helpers.spark_helpers import (
    get_broker_jdbc_url,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
)
from usaspending_api.config import CONFIG
from usaspending_api.download.filestreaming.download_generation import EXCEL_ROW_LIMIT
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.recipient.models import StateData
from usaspending_api.references.models import (
    CGAC,
    NAICS,
    PSC,
    Agency,
    Cfda,
    CityCountyStateCode,
    DisasterEmergencyFundCode,
    GTASSF133Balances,
    ObjectClass,
    Office,
    PopCongressionalDistrict,
    PopCounty,
    RefCountryCode,
    RefProgramActivity,
    SubtierAgency,
    ToptierAgency,
    ZipsGrouped,
)
from usaspending_api.reporting.models import ReportingAgencyMissingTas, ReportingAgencyOverview
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule, SubmissionAttributes

MAX_PARTITIONS = CONFIG.SPARK_MAX_PARTITIONS
_USAS_RDS_REF_TABLES = [
    Agency,
    Cfda,
    CGAC,
    CityCountyStateCode,
    DABSSubmissionWindowSchedule,
    DisasterEmergencyFundCode,
    FederalAccount,
    FinancialAccountsByProgramActivityObjectClass,
    GTASSF133Balances,
    NAICS,
    ObjectClass,
    Office,
    PopCongressionalDistrict,
    PopCounty,
    PSC,
    RefCountryCode,
    RefProgramActivity,
    StateData,
    SubmissionAttributes,
    SubtierAgency,
    ToptierAgency,
    TreasuryAppropriationAccount,
    ReportingAgencyOverview,
    ReportingAgencyMissingTas,
    ZipsGrouped,
]

_BROKER_REF_TABLES = ["cd_state_grouped", "cd_zips_grouped", "cd_county_grouped", "cd_city_grouped"]

logger = logging.getLogger(__name__)


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


def diff(
    left: DataFrame, right: DataFrame, unique_key_col="id", compare_cols=None, include_unchanged_rows=False
) -> DataFrame:
    """Compares two Spark DataFrames that share a schema and returns row-level differences in a DataFrame

    NOTE: Given this returns a DataFrame, two common things to do with it afterwards are:
        1. df.show() ... to print out a display of *some* of the differences. Args to show() include:
                n (int): number of rows to show
                truncate (bool): whether to trim overflowing text in shown results
                vertical (bool): whether to orient data vertically (for perhaps better reading)
            What seems to work well is diff_df.show(n=<how_many>, truncate=False, vertical=True)
        2. df.collect() ... to actually compare ALL rows and then collect all results from the in-memory comparison
           into a Python data structure (List[Row]), for further iteration/manipulation. Use this with CAUTION if the
           result set could be very LARGE, it might overload the memory on your machine

        Or, you can write the resulting DataFrame's data to some other place like a CSV file or database table or Delta
        table

    Args:
        left (DataFrame): DataFrame with a schema that matches the right DataFrame

        right (DataFrame): DataFrame with a schema that matches the right DataFrame

        unique_key_col (str): Column in the dataframes that uniquely identifies the Row, which can be used
            to find matching Rows in each DataFrame. Defaults to "id"

        compare_cols (List[str]): list of columns to be compared, must exist in both left and right DataFrame;
            defaults to None
            list which indicates ALL columns should be compared.
            - If compare_cols is None or an empty list, ALL columns will be compared to each other (for rows where
              unique_key_col values match).
            - Otherwise, specify the columns that determine "sameness" of rows

        include_unchanged_rows (bool): Whether the DataFrame of differences should retain rows that are unchanged;
            default is False

    Returns:
        A DataFrame containing a comparison of the left and right DataFrames. The first column will be named "diff",
        and will have a value of:
        - N = No Change
        - C = Changed. unique_key_column matched for a Row in left and right, but at least one of the columns being
              compared is different from left to right.
        - I = Inserted on the right (not present in the left)
        - D = Deleted on the right (present in left, but not in right)

        Remaining columns returned are the values of the left and right DataFrames' compare_cols.
        Example contents of a returned DataFrame data structure:
            +----+---+---+---------+---------+-----+-----+-----------+-----------+
            |diff|id |id |first_col|first_col|color|color|numeric_val|numeric_val|
            +----+---+---+---------+---------+-----+-----+-----------+-----------+
            |C   |101|101|row 1    |row 1    |blue |blue |-98        |-196       |
            +----+---+---+---------+---------+-----+-----+-----------+-----------+
    """
    if not compare_cols:
        if set(left.schema.names) != set(right.schema.names):
            raise ValueError(
                "The two DataFrames to compare do not contain the same columns. "
                f"\n\tleft cols (in alpha order):  {sorted(set(left.schema.names))} "
                f"\n\tright cols (in alpha order): {sorted(set(right.schema.names))}"
            )
        compare_cols = left.schema.names
    else:
        # Keep the cols to compare ordered as they are defined in the DataFrame schema (the left one to be exact)
        compare_cols = [c for c in left.schema.names if c in compare_cols]

    # unique_key_col does not need to be compared
    if unique_key_col in compare_cols:
        compare_cols.remove(unique_key_col)

    distinct_stmts = " ".join([f"WHEN l.{c} IS DISTINCT FROM r.{c} THEN 'C'" for c in compare_cols])
    compare_expr = f"""
     CASE
         WHEN l.exists IS NULL THEN 'I'
         WHEN r.exists IS NULL THEN 'D'
         {distinct_stmts}
         ELSE 'N'
     END
     """

    differences = (
        left.withColumn("exists", lit(1))
        .alias("l")
        .join(right.withColumn("exists", lit(1)).alias("r"), left[unique_key_col] == right[unique_key_col], "fullouter")
        .withColumn("diff", expr(compare_expr))
    )
    # Put "diff" col first, then follow by the l and r value for each column, for all columns compared
    cols_to_show = (
        ["diff"]
        + [f"l.{unique_key_col}", f"r.{unique_key_col}"]
        + list(chain(*zip([f"l.{c}" for c in compare_cols], [f"r.{c}" for c in compare_cols])))
    )
    differences = differences.select(*cols_to_show)
    if not include_unchanged_rows:
        differences = differences.where("diff != 'N'")
    return differences


def convert_decimal_cols_to_string(df: DataFrame) -> DataFrame:
    df_no_decimal = df
    for f in df.schema.fields:
        if not isinstance(f.dataType, DecimalType):
            continue
        df_no_decimal = df_no_decimal.withColumn(f.name, df_no_decimal[f.name].cast(StringType()))
    return df_no_decimal


def convert_array_cols_to_string(
    df: DataFrame,
    is_postgres_array_format=False,
    is_for_csv_export=False,
) -> DataFrame:
    """For each column that is an Array of ANYTHING, transform it to a string-ified representation of that Array.

    This will:
      1. cast each array element to a STRING representation
      2. Concatenate array elements with ', ' as separator
      3. Wrap the string-concatenation with either square or curly braces (curly if is_postgres_array_format=True)
         - e.g. {"val1", "val2"}

    This is necessary in one case because CSV data source does not support having a DataFrame written to
    CSV if one of the columns of the DataFrame is of data type Array<String>. It will fail without trying. To get
    around this, those column's types must be converted or cast to a String representation before data can be written
    to CSV format.

    Args:
        df (DataFrame): The DataFrame whose Array cols will be string-ified
        is_postgres_array_format (bool): If True, use curly braces to wrap the Array
        is_for_csv_export (bool): Whether the data should be transformed in a way that writes a compatible format in
            CSV files. If True, this has to do a extra handling on the Array in case array values have both/either
            quote ('"') or delimiter (',') characters in their values. The over-cautious approach is to
              1. Quote each element of the Array. This protects against seeing the delimiter character and treating
                 it as a delimiter rather than part of the Array element's value. This is done even if it's destined
                 for an integer[] DB field). Postgres will convert these to the right type upon upload/COPY of the
                 CSV, but consider if this will be a problem for your use of the CSV data; e.g. {"123", "4920"},
                 or {"individual", "business"}
              2. Escape any quotes inside the array element with backslash.
              - A case that involves all of this will yield CSV field value like this when viewed in a text editor,
                assuming Spark CSV options are: quote='"', escape='"' (the default is for it to match quote)
                ...,"{""{\""simple\"": \""elem1\"", \""other\"": \""elem1\""}"", ""{\""simple\"": \""elem2\"", \""other\"": \""elem2\""}""}",...
    """
    arr_open_bracket = "["
    arr_close_bracket = "]"
    if is_postgres_array_format:
        arr_open_bracket = "{"
        arr_close_bracket = "}"
    df_no_arrays = df
    for f in df.schema.fields:
        if not isinstance(f.dataType, ArrayType):
            continue
        df_no_arrays = df_no_arrays.withColumn(
            f.name,
            # Only process NON-NULL values for this Array col. NULL values will be left as NULL
            when(
                col(f.name).isNotNull(),
                # Wrap string-ified array content in brace or bracket
                concat(
                    lit(arr_open_bracket),
                    # Re-join string-ified array elements together with ", "
                    concat_ws(
                        ", ",
                        # NOTE: There does not seem to be a way to cast and enforce that array elements must be NON-NULL
                        #  So NULL array elements would be allowed with this transformation
                        (
                            col(f.name).cast(ArrayType(StringType()))
                            if not is_for_csv_export
                            # When creating CSVs, quote elements and escape inner quotes with backslash
                            else transform(
                                col(f.name).cast(ArrayType(StringType())),
                                lambda c: concat(
                                    lit('"'),
                                    # Special handling in case of data that already has either a quote " or backslash \
                                    # inside an array element
                                    # First replace any single backslash character \ with TWO \\ (an escaped backslash)
                                    # Then replace any quote " character with \" (escaped quote, inside a quoted array elem)
                                    # NOTE: these regexp_replace get sent down to a Java replaceAll, which will require
                                    #       FOUR backslashes to represent ONE
                                    regexp_replace(regexp_replace(c, "\\\\", "\\\\\\\\"), '"', '\\\\"'),
                                    lit('"'),
                                ),
                            )
                        ),
                    ),
                    lit(arr_close_bracket),
                ),
            ),
        )

    return df_no_arrays


def build_ref_table_name_list():
    return [rds_ref_table._meta.db_table for rds_ref_table in _USAS_RDS_REF_TABLES]


def _generate_global_view_sql_strings(tables: List[str], jdbc_url: str) -> List[str]:
    """Generates the CREATE OR REPLACE SQL strings for each of the given tables and JDBC URL"""

    sql_strings: List[str] = []
    jdbc_conn_props = get_jdbc_connection_properties()

    for table_name in tables:
        sql_strings.append(
            f"""
            CREATE OR REPLACE GLOBAL TEMPORARY VIEW {table_name}
            USING JDBC
            OPTIONS (
                driver '{jdbc_conn_props["driver"]}',
                fetchsize '{jdbc_conn_props["fetchsize"]}',
                url '{jdbc_url}',
                dbtable '{table_name}'
            )
            """
        )

    return sql_strings


def create_ref_temp_views(spark: SparkSession, create_broker_views: bool = False):
    """Create global temporary Spark reference views that sit atop remote PostgreSQL RDS tables
    Setting create_broker_views to True will create views for all tables list in _BROKER_REF_TABLES
    Note: They will all be listed under global_temp.{table_name}
    """

    # Create USAS temp views
    rds_ref_tables = build_ref_table_name_list()
    rds_sql_strings = _generate_global_view_sql_strings(
        tables=rds_ref_tables,
        jdbc_url=get_usas_jdbc_url(),
    )
    logger.info(f"Creating the following tables under the global_temp database: {rds_ref_tables}")
    for sql_statement in rds_sql_strings:
        spark.sql(sql_statement)

    # Create Broker temp views
    if create_broker_views:
        broker_sql_strings = _generate_global_view_sql_strings(
            tables=_BROKER_REF_TABLES,
            jdbc_url=get_broker_jdbc_url(),
        )
        logger.info(f"Creating the following Broker tables under the global_temp database: {_BROKER_REF_TABLES}")
        for sql_statement in broker_sql_strings:
            spark.sql(sql_statement)

    logger.info("Created the reference views in the global_temp database")


def write_csv_file(
    spark: SparkSession,
    df: DataFrame,
    parts_dir: str,
    num_partitions: int,
    max_records_per_file=EXCEL_ROW_LIMIT,
    overwrite=True,
    logger=None,
) -> int:
    """Write DataFrame data to CSV file parts.
    Args:
        spark: passed-in active SparkSession
        df: the DataFrame wrapping the data source to be dumped to CSV.
            parts_dir: Path to dir that will contain the outputted parts files from partitions
        num_partitions: Indicates the number of partitions to use when writing the Dataframe
        overwrite: Whether to replace the file CSV files if they already exist by that name
        max_records_per_file: Suggestion to Spark of how many records to put in each written CSV file part,
            if it will end up writing multiple files.
        logger: The logger to use. If one note provided (e.g. to log to console or stdout) the underlying JVM-based
            Logger will be extracted from the ``spark`` ``SparkSession`` and used as the logger.
    Returns:
        record count of the DataFrame that was used to populate the CSV file(s)
    """
    # Delete output data dir if it already exists
    parts_dir_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(parts_dir)
    fs = parts_dir_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(parts_dir_path):
        fs.delete(parts_dir_path, True)
    start = time.time()
    logger.info(f"Writing source data DataFrame to csv part files for file {parts_dir}...")
    df_record_count = df.count()
    df.repartition(num_partitions).write.options(
        # NOTE: this is a suggestion, to be used by Spark if partitions yield multiple files
        maxRecordsPerFile=max_records_per_file,
    ).csv(
        path=parts_dir,
        header=False,
        emptyValue="",  # "" creates the output of ,,, for null values to match behavior of previous Postgres job
        escape='"',  # " is used to escape the 'quote' character setting (which defaults to "). Escaped quote = ""
        ignoreLeadingWhiteSpace=False,  # must set for CSV write, as it defaults to true
        ignoreTrailingWhiteSpace=False,  # must set for CSV write, as it defaults to true
        timestampFormat=CONFIG.SPARK_CSV_TIMEZONE_FORMAT,
        mode="overwrite" if overwrite else "errorifexists",
    )
    logger.info(f"{parts_dir} contains {df_record_count:,} rows of data")
    logger.info(f"Wrote source data DataFrame to csv part files in {(time.time() - start):3f}s")
    return df_record_count


def hadoop_copy_merge(
    spark: SparkSession,
    parts_dir: str,
    header: str,
    part_merge_group_size: int,
    logger=None,
    file_format="csv",
) -> List[str]:
    """PySpark impl of Hadoop 2.x copyMerge() (deprecated in Hadoop 3.x)
    Merges files from a provided input directory and then redivides them
        into multiple files based on merge group size.
    Args:
        spark: passed-in active SparkSession
        parts_dir: Path to the dir that contains the input parts files. The parts dir name
            determines the name of the merged files. Parts_dir cannot have a trailing slash.
        header: A comma-separated list of field names, to be placed as the first row of every final CSV file.
            Individual part files must NOT therefore be created with their own header.
        part_merge_group_size: Final CSV data will be subdivided into numbered files. This indicates how many part files
            should be combined into a numbered file.
        logger: The logger to use. If one note provided (e.g. to log to console or stdout) the underlying JVM-based
            Logger will be extracted from the ``spark`` ``SparkSession`` and used as the logger.
        file_format: The format of the part files and the format of the final merged file, e.g. "csv"

    Returns:
        A list of file paths where each element in the list denotes a path to
            a merged file that was generated during the copy merge.
    """
    overwrite = True
    hadoop = spark.sparkContext._jvm.org.apache.hadoop
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Guard against incorrectly formatted argument value
    parts_dir = parts_dir.rstrip("/")

    parts_dir_path = hadoop.fs.Path(parts_dir)

    fs = parts_dir_path.getFileSystem(conf)

    if not fs.exists(parts_dir_path):
        raise ValueError("Source directory {} does not exist".format(parts_dir))

    file = parts_dir
    file_path = hadoop.fs.Path(file)

    # Don't delete first if disallowing overwrite.
    if not overwrite and fs.exists(file_path):
        raise Py4JError(
            spark._jvm.org.apache.hadoop.fs.FileAlreadyExistsException(f"{str(file_path)} " f"already exists")
        )
    part_files = []

    for f in fs.listStatus(parts_dir_path):
        if f.isFile():
            # Sometimes part files can be empty, we need to ignore them
            if f.getLen() == 0:
                continue
            file_path = f.getPath()
            if file_path.getName().startswith("_"):
                logger.debug(f"Skipping non-part file: {file_path.getName()}")
                continue
            logger.debug(f"Including part file: {file_path.getName()}")
            part_files.append(f.getPath())
    if not part_files:
        logger.warning("Source directory is empty with no part files. Attempting creation of file with CSV header only")
        out_stream = None
        try:
            merged_file_path = f"{parts_dir}.{file_format}"
            out_stream = fs.create(hadoop.fs.Path(merged_file_path), overwrite)
            out_stream.writeBytes(header + "\n")
        finally:
            if out_stream is not None:
                out_stream.close()
        return [merged_file_path]

    part_files.sort(key=lambda f: str(f))  # put parts in order by part number for merging
    paths_to_merged_files = []
    for parts_file_group in _merge_grouper(part_files, part_merge_group_size):
        part_suffix = f"_{str(parts_file_group.part).zfill(2)}" if parts_file_group.part else ""
        partial_merged_file = f"{parts_dir}.partial{part_suffix}"
        partial_merged_file_path = hadoop.fs.Path(partial_merged_file)
        merged_file_path = f"{parts_dir}{part_suffix}.{file_format}"
        paths_to_merged_files.append(merged_file_path)
        # Make path a hadoop path because we are working with a hadoop file system
        merged_file_path = hadoop.fs.Path(merged_file_path)
        if overwrite and fs.exists(merged_file_path):
            fs.delete(merged_file_path, True)
        out_stream = None
        try:
            if fs.exists(partial_merged_file_path):
                fs.delete(partial_merged_file_path, True)
            out_stream = fs.create(partial_merged_file_path)
            out_stream.writeBytes(header + "\n")
            _merge_file_parts(fs, out_stream, conf, hadoop, partial_merged_file_path, parts_file_group.file_list)
        finally:
            if out_stream is not None:
                out_stream.close()
        try:
            fs.rename(partial_merged_file_path, merged_file_path)
        except Exception:
            if fs.exists(partial_merged_file_path):
                fs.delete(partial_merged_file_path, True)
            logger.exception("Exception encountered. See logs")
            raise
    return paths_to_merged_files


def _merge_file_parts(fs, out_stream, conf, hadoop, partial_merged_file_path, part_file_list):
    """Read-in files in alphabetical order and append them one by one to the merged file"""

    for part_file in part_file_list:
        in_stream = None
        try:
            in_stream = fs.open(part_file)
            # Write bytes of each file read and keep out_stream open after write for next file
            hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)
        finally:
            if in_stream:
                in_stream.close()
            if fs.exists(partial_merged_file_path):
                fs.delete(partial_merged_file_path, True)


def _merge_grouper(items, group_size):
    """Helper to chunk up files into mergeable groups"""
    FileMergeGroup = namedtuple("FileMergeGroup", ["part", "file_list"])
    if len(items) <= group_size:
        yield FileMergeGroup(None, items)
        return
    group_generator = (items[i : i + group_size] for i in range(0, len(items), group_size))
    for i, group in enumerate(group_generator, start=1):
        yield FileMergeGroup(i, group)
