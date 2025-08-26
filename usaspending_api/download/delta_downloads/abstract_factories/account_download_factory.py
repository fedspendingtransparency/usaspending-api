from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from typing import TypeVar

from pyspark.sql import functions as sf, Column, SparkSession

from usaspending_api.download.delta_downloads.abstract_downloads.account_download import AbstractAccountDownload
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters


AccountDownload = TypeVar("AccountDownload", bound=AbstractAccountDownload)


class AccountDownloadConditionName(Enum):
    AGENCY = "agency"
    BUDGET_FUNCTION = "budget function"
    BUDGET_SUBFUNCTION = "budget subfunction"
    DEF_CODES = "disaster emergency fund codes"
    FEDERAL_ACCOUNT = "federal account"
    QUARTER_OR_MONTH = "quarter or month"
    YEAR = "year"


class AbstractAccountDownloadFactory(ABC):

    def __init__(self, spark: SparkSession, filters: AccountDownloadFilters):
        self._spark = spark
        self._filters = filters

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @property
    def filters(self) -> AccountDownloadFilters:
        return self._filters

    @property
    def supported_filter_conditions(self) -> list[AccountDownloadConditionName]:
        return list(AccountDownloadConditionName)

    @property
    def dynamic_filters(self) -> Column:
        @dataclass
        class Condition:
            name: AccountDownloadConditionName
            condition: Column
            apply: bool

        conditions = [
            Condition(
                name=AccountDownloadConditionName.YEAR,
                condition=sf.col("reporting_fiscal_year") == self.filters.reporting_fiscal_year,
                apply=True,
            ),
            Condition(
                name=AccountDownloadConditionName.QUARTER_OR_MONTH,
                condition=(
                    (sf.col("reporting_fiscal_period") <= self.filters.reporting_fiscal_period)
                    & ~sf.col("quarter_format_flag")
                )
                | (
                    (sf.col("reporting_fiscal_quarter") <= self.filters.reporting_fiscal_quarter)
                    & sf.col("quarter_format_flag")
                ),
                apply=True,
            ),
            Condition(
                name=AccountDownloadConditionName.AGENCY,
                condition=sf.col("funding_toptier_agency_id") == self.filters.agency,
                apply=bool(self.filters.agency),
            ),
            Condition(
                name=AccountDownloadConditionName.FEDERAL_ACCOUNT,
                condition=sf.col("federal_account_id") == self.filters.federal_account_id,
                apply=bool(self.filters.federal_account_id),
            ),
            Condition(
                name=AccountDownloadConditionName.BUDGET_FUNCTION,
                condition=sf.col("budget_function_code") == self.filters.budget_function,
                apply=bool(self.filters.budget_function),
            ),
            Condition(
                name=AccountDownloadConditionName.BUDGET_SUBFUNCTION,
                condition=sf.col("budget_subfunction_code") == self.filters.budget_subfunction,
                apply=bool(self.filters.budget_subfunction),
            ),
            Condition(
                name=AccountDownloadConditionName.DEF_CODES,
                condition=sf.col("disaster_emergency_fund_code").isin(self.filters.def_codes),
                apply=bool(self.filters.def_codes),
            ),
        ]
        return reduce(
            lambda x, y: x & y,
            [
                condition.condition
                for condition in conditions
                if condition.apply and condition.name in self.supported_filter_conditions
            ],
        )

    @abstractmethod
    def create_federal_account_download(self) -> AccountDownload: ...

    @abstractmethod
    def create_treasury_account_download(self) -> AccountDownload: ...
