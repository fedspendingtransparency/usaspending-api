from pyspark.sql import DataFrame, functions as sf, SparkSession, Window


class LocationDataFrame:

    def __init__(self, spark: SparkSession):
        self.ref_country_code = spark.table("global_temp.ref_country_code")
        # To prevent duplicate state data, we are filtering to the latest year available
        latest_year = 2017
        self.state_data = spark.table("global_temp.state_data").filter(sf.col("year") == latest_year)
        self.ref_city_county_state_code = spark.table("global_temp.ref_city_county_state_code")
        self.zips_grouped = spark.table("global_temp.zips_grouped")
        self.transaction_search = spark.table("rpt.transaction_search")
        self.world_cities = spark.table("raw.world_cities")

    @property
    def country(self):
        return self.ref_country_code.select(
            sf.upper(sf.col("country_name")).alias("location"),
            sf.to_json(sf.struct(sf.col("country_name"), sf.lit("country").alias("location_type"))).alias(
                "location_json"
            ),
            sf.lit("country").alias("location_type"),
        )

    @property
    def state(self):
        return self.state_data.select(
            sf.concat(sf.upper(self.state_data.name), sf.lit(", UNITED STATES")).alias("location"),
            sf.to_json(
                sf.struct(
                    sf.upper(self.state_data.name).alias("state_name"),
                    sf.lit("UNITED STATES").alias("country_name"),
                    sf.lit("state").alias("location_type"),
                )
            ).alias("location_json"),
            sf.lit("state").alias("location_type"),
        )

    @property
    def city_domestic(self):
        return self.ref_city_county_state_code.join(
            self.state_data,
            self.state_data.code == self.ref_city_county_state_code.state_alpha,
        ).select(
            sf.concat(
                sf.upper("feature_name"), sf.lit(", "), sf.upper(self.state_data.name), sf.lit(", UNITED STATES")
            ).alias("location"),
            sf.to_json(
                sf.struct(
                    sf.upper("feature_name").alias("city_name"),
                    sf.upper(self.state_data.name).alias("state_name"),
                    sf.lit("UNITED STATES").alias("country_name"),
                    sf.lit("city").alias("location_type"),
                )
            ).alias("location_json"),
            sf.lit("city").alias("location_type"),
        )

    @property
    def city_foreign(self) -> DataFrame:
        world_cities_tidy = (
            self.world_cities.filter(self.world_cities.iso3 != sf.lit("USA"))
            .withColumn("alt_city_list", sf.split("city_alt", r"\|"))
            .withColumn("alt_cities", sf.explode_outer("alt_city_list"))
            .withColumn("all_city_names_list", sf.array("city", "alt_cities"))
            .withColumn("all_city_names", sf.explode_outer("all_city_names_list"))
            .select("all_city_names", "iso3")
            .dropna()
            .dropDuplicates()
            .withColumnsRenamed({"all_city_names": "city_name", "iso3": "country_code"})
            .select("city_name", "country_code")
        )
        return world_cities_tidy.join(
            self.ref_country_code, world_cities_tidy.country_code == self.ref_country_code.country_code
        ).select(
            sf.concat(sf.upper("city_name"), sf.lit(", "), sf.upper(self.ref_country_code.country_name)).alias(
                "location"
            ),
            sf.to_json(
                sf.struct(
                    sf.upper("city_name").alias("city_name"),
                    sf.lit(None).alias("state_name"),
                    self.ref_country_code.country_name,
                    sf.lit("city").alias("location_type"),
                )
            ).alias("location_json"),
            sf.lit("city").alias("location_type"),
        )

    @property
    def county(self):
        return self.ref_city_county_state_code.join(
            self.state_data,
            self.state_data.code == self.ref_city_county_state_code.state_alpha,
        ).select(
            sf.concat(
                sf.upper(self.ref_city_county_state_code.county_name),
                sf.lit(" COUNTY, "),
                sf.upper(self.state_data.name),
                sf.lit(", UNITED STATES"),
            ).alias("location"),
            sf.to_json(
                sf.struct(
                    sf.upper(self.ref_city_county_state_code.county_name),
                    sf.upper(self.state_data.name).alias("state_name"),
                    sf.lit("UNITED STATES").alias("country_name"),
                    sf.lit("county").alias("location_type"),
                )
            ).alias("location_json"),
            sf.lit("county").alias("location_type"),
        )

    @property
    def zip(self):
        return self.zips_grouped.join(
            self.state_data, self.state_data.code == self.zips_grouped.state_abbreviation
        ).select(
            sf.concat(
                self.zips_grouped.zip5, sf.lit(", "), sf.upper(self.state_data.name), sf.lit(", UNITED STATES")
            ).alias("location"),
            sf.to_json(
                sf.struct(
                    self.zips_grouped.zip5.alias("zip_code"),
                    sf.upper(self.state_data.name).alias("state_name"),
                    sf.lit("UNITED STATES").alias("country_name"),
                    sf.lit("zip_code").alias("location_type"),
                )
            ).alias("location_json"),
            sf.lit("zip_code").alias("location_type"),
        )

    @property
    def current_cd_pop(self):
        return (
            self.transaction_search.join(
                self.state_data, self.state_data.code == self.transaction_search.pop_state_code
            )
            .filter(
                self.transaction_search.pop_state_code.isNotNull()
                & (sf.regexp_extract(self.transaction_search.pop_congressional_code_current, "^[0-9]{2}$", 0) != "")
            )
            .select(
                sf.concat(
                    sf.upper(self.transaction_search.pop_state_code),
                    self.transaction_search.pop_congressional_code_current,
                ).alias("location"),
                sf.to_json(
                    sf.struct(
                        sf.concat(
                            sf.upper("pop_state_code"), sf.lit("-"), sf.col("pop_congressional_code_current")
                        ).alias("current_cd"),
                        sf.upper(self.state_data.name).alias("state_name"),
                        sf.lit("UNITED STATES").alias("country_name"),
                        sf.lit("current_cd").alias("location_type"),
                    )
                ).alias("location_json"),
                sf.lit("current_cd").alias("location_type"),
            )
        )

    @property
    def current_cd_rl(self):
        return (
            self.transaction_search.join(
                self.state_data, self.state_data.code == self.transaction_search.recipient_location_state_code
            )
            .filter(
                self.transaction_search.recipient_location_state_code.isNotNull()
                & (
                    sf.regexp_extract(
                        self.transaction_search.recipient_location_congressional_code_current, "^[0-9]{2}$", 0
                    )
                    != ""
                )
            )
            .select(
                sf.concat(
                    sf.upper(self.transaction_search.recipient_location_state_code),
                    self.transaction_search.recipient_location_congressional_code_current,
                ).alias("location"),
                sf.to_json(
                    sf.struct(
                        sf.concat(
                            sf.upper("recipient_location_state_code"),
                            sf.lit("-"),
                            sf.col("recipient_location_congressional_code_current"),
                        ).alias("current_cd"),
                        sf.upper(self.state_data.name).alias("state_name"),
                        sf.lit("UNITED STATES").alias("country_name"),
                        sf.lit("current_cd").alias("location_type"),
                    )
                ).alias("location_json"),
                sf.lit("current_cd").alias("location_type"),
            )
        )

    @property
    def original_cd_pop(self):
        return (
            self.transaction_search.join(
                self.state_data, self.state_data.code == self.transaction_search.pop_state_code
            )
            .filter(
                self.transaction_search.pop_state_code.isNotNull()
                & (sf.regexp_extract(self.transaction_search.pop_congressional_code, "^[0-9]{2}$", 0) != "")
            )
            .select(
                sf.concat(
                    sf.upper(self.transaction_search.pop_state_code),
                    self.transaction_search.pop_congressional_code,
                ).alias("location"),
                sf.to_json(
                    sf.struct(
                        sf.concat(sf.upper("pop_state_code"), sf.lit("-"), sf.col("pop_congressional_code")).alias(
                            "original_cd"
                        ),
                        sf.upper(self.state_data.name).alias("state_name"),
                        sf.lit("UNITED STATES").alias("country_name"),
                        sf.lit("original_cd").alias("location_type"),
                    )
                ).alias("location_json"),
                sf.lit("original_cd").alias("location_type"),
            )
        )

    @property
    def original_cd_rl(self):
        return (
            self.transaction_search.join(
                self.state_data, self.state_data.code == self.transaction_search.recipient_location_state_code
            )
            .filter(
                self.transaction_search.recipient_location_state_code.isNotNull()
                & (
                    sf.regexp_extract(self.transaction_search.recipient_location_congressional_code, "^[0-9]{2}$", 0)
                    != ""
                )
            )
            .select(
                sf.concat(
                    sf.upper(self.transaction_search.recipient_location_state_code),
                    self.transaction_search.recipient_location_congressional_code,
                ).alias("location"),
                sf.to_json(
                    sf.struct(
                        sf.concat(
                            sf.upper("recipient_location_state_code"),
                            sf.lit("-"),
                            sf.col("recipient_location_congressional_code"),
                        ).alias("original_cd"),
                        sf.upper(self.state_data.name).alias("state_name"),
                        sf.lit("UNITED STATES").alias("country_name"),
                        sf.lit("original_cd").alias("location_type"),
                    )
                ).alias("location_json"),
                sf.lit("original_cd").alias("location_type"),
            )
        )

    @property
    def dataframe(self):
        union_all = (
            self.country.union(self.state)
            .union(self.city_domestic)
            .union(self.city_foreign)
            .union(self.county)
            .union(self.zip)
            .union(self.current_cd_pop)
            .union(self.current_cd_rl)
            .union(self.original_cd_pop)
            .union(self.original_cd_rl)
            .dropDuplicates()
        )
        w = Window.orderBy(union_all.location, union_all.location_json)
        return union_all.withColumn("id", sf.row_number().over(w))
