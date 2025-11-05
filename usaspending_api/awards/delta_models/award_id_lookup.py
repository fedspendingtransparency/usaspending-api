from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType


AWARD_ID_LOOKUP_SCHEMA = StructType(
    [
        StructField("award_id", LongType(), False),
        StructField("is_fpds", BooleanType(), False),
        StructField("transaction_unique_id", StringType(), False),
        StructField("generated_unique_award_id", StringType(), False),
    ]
)
