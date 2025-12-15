from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import cached_property
from typing import TypeVar

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pydantic import BaseModel
from pyspark.sql import Column, DataFrame, SparkSession

DownloadFilters = TypeVar("DownloadFilters", bound=BaseModel)


class AbstractDownload(ABC):
    def __init__(
        self,
        spark: SparkSession | DuckDBSparkSession,
        filters: DownloadFilters,
        dynamic_filters: Column | DuckDBSparkColumn,
    ):
        self._spark = spark
        self._filters = filters
        self._dynamic_filters = dynamic_filters
        self._start_time = datetime.now(timezone.utc)

    @property
    def filters(self) -> DownloadFilters:
        return self._filters

    @property
    def dynamic_filters(self) -> Column | DuckDBSparkColumn:
        return self._dynamic_filters

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def spark(self) -> SparkSession | DuckDBSparkSession:
        return self._spark

    @cached_property
    def file_name(self) -> str:
        return self._build_file_name()

    @cached_property
    def dataframe(self) -> DataFrame | DuckDBSparkDataFrame:
        return self._build_dataframe()

    @abstractmethod
    def _build_file_name(self) -> str: ...

    @abstractmethod
    def _build_dataframe(self) -> DataFrame | DuckDBSparkDataFrame: ...
