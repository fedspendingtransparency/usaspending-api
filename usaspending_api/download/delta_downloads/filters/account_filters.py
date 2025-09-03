import warnings
from typing import Any, Literal

from pydantic import BaseModel, root_validator, validator
from pydantic.fields import ModelField

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import ToptierAgency


class AccountDownloadFilters(BaseModel):
    fy: int
    submission_types: list[Literal["account_balances", "object_class_program_activity", "award_financial"]]
    period: int | None = None
    quarter: int | None = None
    agency: int | None = None
    federal_account: int | None = None
    budget_function: str | None = None
    budget_subfunction: str | None = None
    def_codes: list[str] | None = None

    @property
    def federal_account_id(self) -> int | None:
        return self.federal_account

    @property
    def reporting_fiscal_year(self) -> int:
        return self.fy

    @property
    def reporting_fiscal_quarter(self) -> int:
        return self.quarter or self.period // 3

    @property
    def reporting_fiscal_period(self) -> int:
        return self.period or self.quarter * 3

    @validator("fy", "period", "quarter", "agency", "federal_account", pre=True)
    @classmethod
    def ensure_int_or_none(cls, value: Any, field: ModelField) -> Any:
        if value == "all":
            result = None
        elif value is None:
            result = value
        elif not isinstance(value, int):
            try:
                result = int(value)
            except ValueError:
                raise InvalidParameterException(f"{field.name} must be an integer.")
        else:
            result = value
        return result

    @validator("budget_function", "budget_subfunction", pre=True)
    @classmethod
    def check_for_all(cls, value: Any) -> Any:
        if value == "all":
            return None
        else:
            return value

    @validator("agency")
    @classmethod
    def check_agency_exists(cls, value: Any) -> Any:
        if value is not None and not ToptierAgency.objects.filter(toptier_agency_id=value).exists():
            raise InvalidParameterException("Agency with that ID does not exist")
        return value

    @validator("federal_account")
    @classmethod
    def check_federal_account_exists(cls, value: Any) -> Any:
        if value is not None and not FederalAccount.objects.filter(id=value).exists():
            raise InvalidParameterException("Federal Account with that ID does not exist")
        return value

    @root_validator
    @classmethod
    def check_period_quarter(cls, values: dict[str, Any]) -> dict[str, Any]:
        period, quarter = values.get("period"), values.get("quarter")
        if period is None and quarter is None:
            raise InvalidParameterException("Must define period or quarter.")
        if period is not None and quarter is not None:
            values["quarter"] = quarter = None
            warnings.warn("Both quarter and period are set.  Only using period.")
        if period is not None and period not in range(2, 13):
            raise InvalidParameterException("Period must be between 2 and 12")
        if quarter is not None and quarter not in range(1, 5):
            raise InvalidParameterException("Quarter must be between 1 and 4")
        return values

    @root_validator
    @classmethod
    def check_submission_type_defc(cls, values: dict[str, Any]) -> dict[str, Any]:
        includes_account_balances = "account_balances" in values.get("submission_types", [])
        has_defc = bool(values.get("def_codes"))
        if includes_account_balances and has_defc:
            warnings.warn(
                "Account balances can not be filtered by def code; this filter will be ignored for account balances."
            )
        return values
