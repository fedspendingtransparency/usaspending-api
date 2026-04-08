from abc import abstractmethod
from enum import Enum

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pyspark.sql import DataFrame

from usaspending_api.download.delta_downloads.abstract_downloads.base_download import (
    AbstractDownload,
)


class Category(str, Enum):
    ASSISTANCE = ("assistance", "Assistance")
    CONTRACT = ("contract", "Contracts")

    def __new__(cls, category: str, title: str):
        obj = str.__new__(cls, category)
        obj._value_ = category
        obj._title = title
        return obj

    @property
    def title(self) -> str:
        return self._title


class MonthlyType(str, Enum):
    DELTA = ("delta", "Delta")
    FULL = ("full", "Full")

    def __new__(cls, monthly_type: str, title: str):
        obj = str.__new__(cls, monthly_type)
        obj._value_ = monthly_type
        obj._title = title
        return obj

    @property
    def title(self) -> str:
        return self._title


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
    def category(self) -> Category: ...

    @property
    @abstractmethod
    def monthly_type(self) -> MonthlyType: ...

    @property
    def download_table(self) -> DataFrame | DuckDBSparkDataFrame:
        return self.spark.table("rpt.transaction_download")

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
            case Category.ASSISTANCE:
                category_filter = ~self.sf.col("is_fpds")
            case Category.CONTRACT:
                category_filter = self.sf.col("is_fpds")
            case _:
                raise NotImplementedError(f"Dynamic filters doesn't support category of '{self.category}'")

        self._dynamic_filters &= category_filter

    def _build_file_names(self) -> list[str]:
        agency = (
            self.filters.awarding_toptier_agency_code if self.filters.awarding_toptier_agency_abbreviation else "All"
        )
        category = self.category.title
        monthly_type = self.monthly_type.title
        year = self.filters.fiscal_year or "(All)"
        return [f"FY{year}_{agency}_{category}_{monthly_type}_{self.filters.as_of_date}"]
