from pyspark.sql import Column, functions as sf


def fy_quarter_period() -> Column:
    return sf.when(
        sf.col("quarter_format_flag"),
        sf.concat(sf.lit("FY"), sf.col("reporting_fiscal_year"), sf.lit("Q"), sf.col("reporting_fiscal_quarter")),
    ).otherwise(
        sf.concat(
            sf.lit("FY"),
            sf.col("reporting_fiscal_year"),
            sf.lit("P"),
            sf.lpad(sf.col("reporting_fiscal_period"), 2, "0"),
        )
    )
