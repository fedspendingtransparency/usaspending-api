from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from usaspending_api.common.helpers.spark_helpers import get_jvm_logger


def extract_db_data_frame(
    spark: SparkSession,
    conn_props: dict,
    jdbc_url: str,
    partition_rows: int,
    min_max_sql: str,
    table: str,
    partitioning_col: str,
    is_numeric_partitioning_col: bool = True,
) -> DataFrame:  # pragma: no cover -- will be used and tested eventually
    logger = get_jvm_logger(spark)

    # Get the bounds of the data we are extracting, so we can let spark partition it
    min_max_df = spark.read.jdbc(url=jdbc_url, table=min_max_sql, properties=conn_props)
    min_max = min_max_df.first()
    min_id = min_max[0]
    max_id = min_max[1]
    count = min_max[2]

    if is_numeric_partitioning_col:
        # Take count as partition if using a spotty range, and count of rows is less than range of IDs
        partitions = int(min((max_id - min_id), count) / (partition_rows + 1))
        logger.info(f"Derived {partitions} partitions from numeric ranges across column: {partitioning_col}")

        data_df = spark.read.jdbc(
            url=jdbc_url,
            table=table,
            column=partitioning_col,
            lowerBound=min_id,
            upperBound=max_id,
            numPartitions=partitions,
            properties=conn_props,
        )
    else:
        partitions = int(count / (partition_rows + 1))
        logger.info(
            f"Derived {partitions} partitions from {count} distinct non-numeric (text) "
            f"values in column: {partitioning_col}."
        )

        # SQL usable in Postgres to get a distinct 32-bit int from an md5 hash of text
        pg_int_from_hash = f"('x'||substr(md5({partitioning_col}),1,8))::bit(32)::int"
        # int could be signed. This workaround SQL gets unsigned modulus from the hash int
        non_neg_modulo = f"mod({partitions} + mod({pg_int_from_hash}, {partitions}), {partitions})"
        partition_sql_predicates = [f"{non_neg_modulo} = {p}" for p in range(0, partitions)]

        data_df = spark.read.jdbc(
            url=jdbc_url,
            table=table,
            predicates=partition_sql_predicates,
            properties=conn_props,
        )

    logger.info(f"Partitions will have approximately {partition_rows} rows each.")
    return data_df


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
