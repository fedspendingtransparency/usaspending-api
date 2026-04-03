import logging
from abc import ABC, abstractmethod
from typing import TypeVar

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from pyspark.sql import Column, SparkSession

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import (
    AbstractMonthlyDownload,
    MonthlyType,
)
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import (
    MonthlyDownloadFilters,
)

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
        # Placeholder to allow conditionally building the filters
        result_filters = self.sf.lit(True)

        if self.filters.awarding_toptier_agency_code is not None:
            result_filters &= self.sf.col("awarding_agency_code") == self.filters.awarding_toptier_agency_code

        if self.filters.fiscal_year is not None:
            result_filters &= self.sf.col("action_date_fiscal_year") == self.filters.fiscal_year

        return result_filters

    @abstractmethod
    def _create_delta_download(self) -> MonthlyDownload: ...

    @abstractmethod
    def _create_full_download(self) -> MonthlyDownload: ...

    def get_download(self, monthly_type: MonthlyType) -> MonthlyDownload:
        match monthly_type:
            case MonthlyType.DELTA:
                download = self._create_delta_download()
            case MonthlyType.FULL:
                download = self._create_full_download()
            case _:
                raise InvalidParameterException(f"Unsupported monthly type: {monthly_type}")

        return download
