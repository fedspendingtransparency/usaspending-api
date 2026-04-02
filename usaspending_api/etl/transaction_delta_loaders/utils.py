from pyspark.sql import Column, functions as sf


def parse_date_column(column: str, table: str | None = None, is_casted_to_date: bool = True) -> Column:
    column_ref = sf.col((f"{table}." if table else "") + column)
    regexp_mmddYYYY = r"(\d{2})(?<sep>[-/])(\d{2})(\k<sep>)(\d{4})(.\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2})?)?"
    regexp_YYYYmmdd = r"(\d{4})(?<sep>[-/]?)(\d{2})(\k<sep>)(\d{2})(.\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2})?)?"
    mmddYYYY_fmt = sf.concat(
        sf.regexp_extract(column_ref, regexp_mmddYYYY, 5),
        sf.lit("-"),
        sf.regexp_extract(column_ref, regexp_mmddYYYY, 1),
        sf.lit("-"),
        sf.regexp_extract(column_ref, regexp_mmddYYYY, 3),
    )
    YYYYmmdd_fmt = sf.concat(
        sf.regexp_extract(column_ref, regexp_YYYYmmdd, 1),
        sf.lit("-"),
        sf.regexp_extract(column_ref, regexp_YYYYmmdd, 3),
        sf.lit("-"),
        sf.regexp_extract(column_ref, regexp_YYYYmmdd, 5),
    )
    if is_casted_to_date:
        mmddYYYY_fmt = mmddYYYY_fmt.cast("date")
        YYYYmmdd_fmt = YYYYmmdd_fmt.cast("date")
    return sf.when(sf.regexp_extract(column_ref, regexp_mmddYYYY, 0) != sf.lit(""), mmddYYYY_fmt).otherwise(
        YYYYmmdd_fmt
    )
