from abc import abstractmethod
from datetime import datetime
from enum import Enum

from usaspending_api.download.delta_downloads.abstract_downloads.base_download import AbstractDownload
from usaspending_api.download.download_utils import construct_data_date_range, obtain_filename_prefix_from_agency_id


class AccountLevel(str, Enum):
    FEDERAL_ACCOUNT = ("federal_account", "FA")
    TREASURY_ACCOUNT = ("treasury_account", "TAS")

    def __new__(cls, account_level: str, abbreviation: str):
        obj = str.__new__(cls, account_level)
        obj._value_ = account_level
        obj._abbreviation = abbreviation
        return obj

    @property
    def abbreviation(self):
        return self._abbreviation


class SubmissionType(str, Enum):
    ACCOUNT_BALANCES = ("account_balances", "AccountBalances")
    AWARD_FINANCIAL = ("award_financial", "AccountBreakdownByAward")
    OBJECT_CLASS_PROGRAM_ACTIVITY = ("object_class_program_activity", "AccountBreakdownByPA-OC")

    def __new__(cls, submission_type: str, title: str):
        obj = str.__new__(cls, submission_type)
        obj._value_ = submission_type
        obj._title = title
        return obj

    @property
    def title(self):
        return self._title


class AbstractAccountDownload(AbstractDownload):

    @property
    @abstractmethod
    def account_level(self) -> AccountLevel: ...

    @property
    @abstractmethod
    def submission_type(self) -> SubmissionType: ...

    def _build_file_names(self) -> list[str]:
        date_range = construct_data_date_range(self.filters.dict())
        agency = obtain_filename_prefix_from_agency_id(self.filters.agency)
        level = self.account_level.abbreviation
        title = self.submission_type.title
        timestamp = datetime.strftime(self.start_time, "%Y-%m-%d_H%HM%MS%S")
        return [f"{date_range}_{agency}_{level}_{title}_{timestamp}"]
