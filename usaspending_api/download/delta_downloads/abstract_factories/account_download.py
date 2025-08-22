from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

from usaspending_api.download.download_utils import construct_data_date_range, obtain_filename_prefix_from_agency_id

if TYPE_CHECKING:
    from typing import TypeVar
    from pydantic import BaseModel
    from pyspark.sql import Column, DataFrame, SparkSession

DownloadFilters = TypeVar("DownloadFilters", bound=BaseModel)


class AccountDownloadLevel(str, Enum):
    FEDERAL_ACCOUNT = ("federal_account", "FA")
    TREASURY_ACCOUNT = ("treasury_account", "TAS")

    def __new__(cls, account_level: str, abbreviation: str):
        obj = str.__new__(cls, account_level)
        obj._value = account_level
        obj._abbreviation = abbreviation
        return obj

    @property
    def abbreviation(self):
        return self._abbreviation


class AccountDownloadType(str, Enum):
    ACCOUNT_BALANCES = ("account_balances", "AccountBalances")
    AWARD_FINANCIAL = ("award_financial", "AccountBreakdownByAward")
    OBJECT_CLASS_PROGRAM_ACTIVITY = ("object_class_program_activity", "AccountBreakdownByPA-OC")

    def __new__(cls, submission_type: str, title: str):
        obj = str.__new__(cls, submission_type)
        obj._value = submission_type
        obj._title = title
        return obj

    @property
    def title(self):
        return self._title


class AbstractAccountDownload(ABC):

    def __init__(self, spark: SparkSession, filters: DownloadFilters, dynamic_filters: Column):
        self._spark = spark
        self._filters = filters
        self._dynamic_filters = dynamic_filters
        self._start_time = datetime.now(timezone.utc)

    @property
    @abstractmethod
    def download_level(self) -> AccountDownloadLevel: ...

    @property
    @abstractmethod
    def download_type(self) -> AccountDownloadType: ...

    @property
    @abstractmethod
    def dataframe(self) -> DataFrame: ...

    @property
    def spark(self) -> SparkSession:
        return self._spark

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
    def file_name(self) -> str:
        date_range = construct_data_date_range(self.filters.dict())
        agency = obtain_filename_prefix_from_agency_id(self.filters.agency)
        level = self.download_level.abbreviation
        title = self.download_type.title
        timestamp = datetime.strftime(self.start_time, "%Y-%m-%d_H%HM%MS%S%f")
        return f"{date_range}_{agency}_{level}_{title}_{timestamp}"
