from datetime import date, datetime
from typing import Any

from django.utils.functional import cached_property
from pydantic import BaseModel, validator
from pydantic.v1.fields import ModelField

from usaspending_api.references.models import ToptierAgency


class MonthlyDownloadFilters(BaseModel):
    as_of_date: str | None
    awarding_toptier_agency_abbreviation: str | None = None
    fiscal_year: int | None = None

    class Config:
        # TODO: Will need to update to use ConfigDict's "ignored_types" after Pydantic upgrade
        #       https://docs.pydantic.dev/latest/api/config/?query=ignore_types#pydantic.config.ConfigDict.ignored_types
        keep_untouched = (cached_property,)

    @validator("as_of_date", pre=True, always=True)
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

    @cached_property
    def awarding_toptier_agency_code(self) -> str:
        result = self.awarding_toptier_agency_abbreviation
        if result is not None:
            result = (
                ToptierAgency.objects.filter(abbreviation=self.awarding_toptier_agency_abbreviation)
                .values_list("toptier_code", flat=True)
                .first()
            )
        return result

    @validator("awarding_toptier_agency_abbreviation")
    @classmethod
    def check_valid_toptier_agency_abbreviation(cls, abbreviation: str, field: ModelField) -> str:
        abbreviation = abbreviation.upper()
        if not ToptierAgency.objects.filter(abbreviation=abbreviation).exists():
            raise ValueError(f"Invalid abbreviation for '{field.name}': {abbreviation}")

        return abbreviation

    @validator("fiscal_year")
    @classmethod
    def check_valid_fiscal_year(cls, fiscal_year: int) -> int:
        if fiscal_year < 2008:
            raise ValueError(f"Fiscal year of '{fiscal_year}' is below the minimum of 2008")
        return fiscal_year
