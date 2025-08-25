from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from pydantic import BaseModel
    from pyspark.sql import Column, DataFrame, SparkSession

    DownloadFilters = TypeVar("DownloadFilters", bound=BaseModel)


class AbstractDownload(ABC):
    def __init__(self, spark: SparkSession, filters: DownloadFilters, dynamic_filters: Column):
        self._spark = spark
        self._filters = filters
        self._dynamic_filters = dynamic_filters
        self._start_time = datetime.now(timezone.utc)

    @property
    @abstractmethod
    def file_name(self) -> str: ...

    @property
    @abstractmethod
    def dataframe(self) -> DataFrame: ...

    @property
    def filters(self) -> DownloadFilters:
        return self._filters

    @property
    def dynamic_filters(self) -> Column:
        return self._dynamic_filters

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def spark(self) -> SparkSession:
        return self._spark
