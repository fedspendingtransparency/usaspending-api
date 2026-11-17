from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as sf

LOCATION_COMPONENTS = [
    "country_name",
    "country_code",
    "state_name",
    "state_abbreviation",
    "city_name",
    "county_name",
    "county_fips",
    "zip_code",
    "congressional_district",
]

LEGACY_JSON_KEYS = {"congressional_district": None}

US_DATA = {"country_name": "UNITED STATES", "country_code": "USA"}


def _doc(df: DataFrame, location: Column, location_type: str, legacy_json: dict = None, **components) -> DataFrame:
    """Selects the common document shape. Components not given are set to null."""
    if legacy_json is None:
        legacy_json = {}
    comps = {c: sf.lit(components.get(c, None)).cast("string") for c in LOCATION_COMPONENTS}
    json_cols = [v.alias(k) for k, v in comps.items()]  # null keys are dropped in to_json
    json_cols += [v.alias(k) for k, v in legacy_json.items()]
    json_cols.append(sf.lit(location_type).alias("location_type"))
    return df.select(
        location.alias("location"),
        sf.to_json(sf.struct(*json_cols)).alias("location_json"),
        sf.lit(location_type).alias("location_type"),
        *[v.alias(k) for k, v in comps.items()],
    )


class LocationDataFrame:
    def __init__(self, spark: SparkSession):
        self.ref_country_code = spark.table("global_temp.ref_country_code")
        # To prevent duplicate state data, we are filtering to the latest year available
        w = Window.partitionBy(sf.col("fips"))
        self.state_data = (
            spark.table("global_temp.state_data")
            .withColumn("max_year", sf.max("year").over(w))
            .filter(sf.col("year") == sf.col("max_year"))
            .drop(sf.col("max_year"))
        )
        self.ref_city_county_state_code = spark.table("global_temp.ref_city_county_state_code")
        self.zips_grouped = spark.table("global_temp.zips_grouped")
        self.transaction_search = spark.table("rpt.transaction_search")
        self.world_cities = spark.table("raw.world_cities")

    @property
    def country(self) -> DataFrame:
        ref_code = self.ref_country_code
        return _doc(
            ref_code,
            sf.upper("country_name"),
            "country",
            country_name=sf.upper("country_name"),
            country_code=ref_code.country_code,
        )

    @property
    def state(self) -> DataFrame:
        state = self.state_data
        return _doc(
            state,
            sf.concat(sf.upper(state.name), sf.lit(", UNITED STATES")).alias("location"),
            "state",
            state_name=sf.upper(state.name),
            state_abbreviation=sf.upper(state.code),
            **US_DATA,
        )

    @property
    def city_domestic(self) -> DataFrame:
        ref, state = self.ref_city_county_state_code, self.state_data
        return _doc(
            ref.join(state, state.code == ref.state_alpha),
            sf.concat(sf.upper("feature_name"), sf.lit(", "), sf.upper(state.name), sf.lit(", UNITED STATES")),
            "city",
            city_name=sf.upper("feature_name"),
            state_name=sf.upper(state.name),
            state_abbreviation=sf.upper(state.code),
            **US_DATA,
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
        rc = self.ref_country_code
        city_data = world_cities_tidy.join(rc, world_cities_tidy.country_code == rc.country_code)
        return _doc(
            city_data,
            sf.concat(sf.upper("city_name"), sf.lit(", "), sf.upper(rc.country_name)),
            "city",
            city_name=sf.upper("city_name"),
            country_name=sf.upper(rc.country_name),
            country_code=rc.country_code,
        )

    @property
    def county(self) -> DataFrame:
        ref, state = self.ref_city_county_state_code, self.state_data
        return _doc(
            ref.join(state, state.code == ref.state_alpha),
            sf.concat(sf.upper(ref.county_name), sf.lit(" COUNTY, "), sf.upper(state.name), sf.lit(", UNITED STATES")),
            "county",
            county_name=sf.upper(ref.county_name),
            county_fips=sf.concat(ref.state_numeric, ref.county_numeric),
            state_name=sf.upper(state.name),
            state_abbreviation=sf.upper(state.code),
            **US_DATA,
        )

    @property
    def zip(self) -> DataFrame:
        zips, state = self.zips_grouped, self.state_data
        return _doc(
            zips.join(state, state.code == zips.state_abbreviation),
            sf.concat(zips.zip5, sf.lit(", "), sf.upper(state.name), sf.lit(", UNITED STATES")),
            "zip_code",
            zip_code=zips.zip5,
            state_name=sf.upper(state.name),
            state_abbreviation=sf.upper(state.code),
            **US_DATA,
        )

    def _cd(self, state_col: Column, code_col: Column, location_type: str) -> DataFrame:
        t, state = self.transaction_search, self.state_data
        cd = sf.concat(sf.upper(state_col), sf.lit("-"), code_col)
        return _doc(
            t.join(state, state.code == state_col).filter(
                state_col.isNotNull() & (sf.regexp_extract(code_col, "^[0-9]{2}$", 0) != "")
            ),
            sf.concat(sf.upper(state_col), code_col),
            location_type,
            legacy_json={location_type: cd},
            congressional_district=cd,
            state_name=sf.upper(state.name),
            state_abbreviation=sf.upper(state.code),
            **US_DATA,
        )

    @property
    def current_cd_pop(self) -> DataFrame:
        t = self.transaction_search
        return self._cd(t.pop_state_code, t.pop_congressional_code_current, "current_cd")

    @property
    def current_cd_rl(self) -> DataFrame:
        t = self.transaction_search
        return self._cd(t.recipient_location_state_code, t.recipient_location_congressional_code_current, "current_cd")

    @property
    def original_cd_pop(self) -> DataFrame:
        t = self.transaction_search
        return self._cd(t.pop_state_code, t.pop_congressional_code, "original_cd")

    @property
    def original_cd_rl(self) -> DataFrame:
        t = self.transaction_search
        return self._cd(t.recipient_location_state_code, t.recipient_location_congressional_code, "original_cd")

    @property
    def dataframe(self) -> DataFrame:
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
