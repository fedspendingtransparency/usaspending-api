from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType


TRANSACTION_ID_LOOKUP_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), False),
        StructField("is_fpds", BooleanType(), False),
        StructField("transaction_unique_id", StringType(), False),
    ]
)
