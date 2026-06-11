import logging
from abc import ABC, abstractmethod
from typing import TypeVar

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from pyspark.sql import Column, SparkSession

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import (
    AbstractMonthlyDownload,
)
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import (
    MonthlyDownloadFilters,
)
from usaspending_api.download.delta_downloads.helpers.enums import MonthlyType

logger = logging.getLogger(__name__)

MonthlyDownload = TypeVar("MonthlyDownload", bound=AbstractMonthlyDownload)


class AbstractMonthlyDownloadFactory(ABC):
    def __init__(self, spark: SparkSession, filters: MonthlyDownloadFilters):
        self._spark = spark
        self._filters = filters

        if isinstance(self.spark, DuckDBSparkSession):
            from duckdb.experimental.spark.sql import functions
        else:
            from pyspark.sql import functions

        self.sf = functions

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @property
    def filters(self) -> MonthlyDownloadFilters:
        return self._filters

    @property
    def dynamic_filters(self) -> Column | DuckDBSparkColumn:
        """Build dynamic filters that apply to both FULL and DELTA downloads"""
        result_filters = self.sf.lit(True)

        # Agency filter
        if self.filters.awarding_toptier_agency_code is not None:
            result_filters &= self.sf.col("awarding_agency_code") == self.filters.awarding_toptier_agency_code

        # Fiscal year filter (only for FULL downloads)
        if self.filters.fiscal_year is not None:
            result_filters &= self.sf.col("action_date_fiscal_year") == self.filters.fiscal_year

        return result_filters

    def get_delta_filters(self) -> Column | DuckDBSparkColumn:
        """
        Build filters specific to DELTA downloads.
        Returns records that have been modified since delta_start_date.
        """
        if not self.filters.delta_start_date:
            raise ValueError("delta_start_date is required for DELTA downloads")

        # Convert YYYY-MM-DD to date type for comparison
        delta_date = self.sf.to_date(self.sf.lit(self.filters.delta_start_date), "yyyy-MM-dd")

        # Get records modified since delta_start_date
        # Records are considered "delta" if:
        # 1. They were updated/created after the delta_start_date (etl_update_date >= delta_start_date)
        # 2. OR they are marked as corrections in the TransactionDelta table
        delta_filters = (
                                self.sf.col("etl_update_date") >= delta_date
                        ) | (
                            self.sf.col("transaction_delta_id").isNotNull()
                        )

        return delta_filters

    @abstractmethod
    def _create_delta_download(self) -> MonthlyDownload:
        ...

    @abstractmethod
    def _create_full_download(self) -> MonthlyDownload:
        ...

    def get_download(self, monthly_type: MonthlyType) -> MonthlyDownload:
        match monthly_type:
            case MonthlyType.DELTA:
                download = self._create_delta_download()
            case MonthlyType.FULL:
                download = self._create_full_download()
            case _:
                raise InvalidParameterException(f"Unsupported monthly type: {monthly_type}")

        return download
