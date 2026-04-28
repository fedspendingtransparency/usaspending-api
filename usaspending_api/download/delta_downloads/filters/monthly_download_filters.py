from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

from usaspending_api.references.models import ToptierAgency


class MonthlyDownloadFilters(BaseModel):

    as_of_date: str | None = None
    awarding_toptier_agency_code: str | None = None
    fiscal_year: int | None = None

    @field_validator("as_of_date", mode='before')
    @classmethod
    def validate_as_of_date_and_set_default(cls, value: Any) -> str:
        if isinstance(value, str):
            err_msg = "'as_of_date' must be in the format yyyyMMdd"
            if len(value) == 8:
                try:
                    datetime.strptime(value, "%Y%m%d")
                except ValueError as err:
                    raise ValueError(err_msg) from err
            else:
                raise ValueError(err_msg)
        elif value is None:
            value = date.today().strftime("%Y%m%d")
        else:
            raise ValueError(f"Received unsupported type of '{type(value)}'; expected 'str'")
        return value

    @field_validator("awarding_toptier_agency_code")
    @classmethod
    def check_valid_toptier_agency_code(cls, toptier_code: str, info: ValidationInfo) -> str:
        if not ToptierAgency.objects.filter(toptier_code=toptier_code).exists():
            raise ValueError(f"Invalid toptier code for '{info.field_name}': {toptier_code}")

        return toptier_code

    @field_validator("fiscal_year")
    @classmethod
    def check_valid_fiscal_year(cls, fiscal_year: int) -> int:
        if fiscal_year < 2008:
            raise ValueError(f"Fiscal year of '{fiscal_year}' is below the minimum of 2008")
        return fiscal_year
