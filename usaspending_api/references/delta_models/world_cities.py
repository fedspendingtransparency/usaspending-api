from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("city", StringType(), False),
        StructField("city_ascii", StringType()),
        StructField("city_alt", StringType()),
        StructField("city_local", StringType()),
        StructField("city_local_lang", StringType()),
        StructField("lat", DecimalType(6, 4), False),
        StructField("lng", DecimalType(7, 4), False),
        StructField("country", StringType(), False),
        StructField("iso2", StringType(), False),
        StructField("iso3", StringType(), False),
        StructField("admin_name", StringType()),
        StructField("admin_name_ascii", StringType()),
        StructField("admin_code", StringType()),
        StructField("admin_type", StringType()),
        StructField("capital", StringType()),
        StructField("density", FloatType(), False),
        StructField("population", IntegerType()),
        StructField("population_proper", IntegerType()),
        StructField("ranking", StringType(), False),
        StructField("timezone", StringType(), False),
        StructField("same_name", BooleanType(), False),
        StructField("id", LongType(), False),
    ]
)
