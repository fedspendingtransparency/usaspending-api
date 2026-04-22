from abc import abstractmethod

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pyspark.sql import DataFrame

from usaspending_api.download.delta_downloads.abstract_downloads.base_download import (
    AbstractDownload,
)
from usaspending_api.download.delta_downloads.helpers.enums import AwardCategory, MonthlyType


class AbstractMonthlyDownload(AbstractDownload):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.spark, DuckDBSparkSession):
            from duckdb.experimental.spark.sql import functions
        else:
            from pyspark.sql import functions

        self.sf = functions

        # TODO: Update Monthly Delta download to limit results to only recent changes
        #       (see populate_monthly_delta_files.py)
        self._validate_filters_for_monthly_type()
        self._add_category_filter()

    @property
    @abstractmethod
    def category(self) -> AwardCategory: ...

    @property
    @abstractmethod
    def monthly_type(self) -> MonthlyType: ...

    @property
    def download_table(self) -> DataFrame | DuckDBSparkDataFrame:
        return self.spark.table("rpt.transaction_download")

    @property
    def file_name_prefix(self) -> str:
        agency = (
            self.filters.awarding_toptier_agency_code or "All"
        )
        category = self.category.title
        monthly_type = self.monthly_type.title
        year = self.filters.fiscal_year or "(All)"
        return f"FY{year}_{agency}_{category}_{monthly_type}"

    def _validate_filters_for_monthly_type(self) -> None:
        """Special handling of the filters due to business logic, not technical limitations"""
        match self.monthly_type:
            case MonthlyType.DELTA:
                if self.filters.fiscal_year is not None:
                    raise ValueError("'fiscal_year' is not supported for monthly_type of 'DELTA'")
            case MonthlyType.FULL:
                if self.filters.fiscal_year is None:
                    raise ValueError("'fiscal_year' is required for monthly_type of 'FULL'")

    def _add_category_filter(self) -> None:
        match self.category:
            case AwardCategory.ASSISTANCE:
                category_filter = ~self.sf.col("is_fpds")
            case AwardCategory.CONTRACT:
                category_filter = self.sf.col("is_fpds")
            case _:
                raise NotImplementedError(f"Dynamic filters doesn't support category of '{self.category}'")

        self._dynamic_filters &= category_filter

    def _build_file_names(self) -> list[str]:
        return [f"{self.file_name_prefix}_{self.filters.as_of_date}"]
