from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator, model_validator


class TimePeriod(BaseModel):
    start_date: str
    end_date: str
    date_type: Literal["action_date", "date_signed", "last_modified_date", "new_awards_only"] | None = None

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
        return v

    @model_validator(mode="after")
    def validate_end_date_is_after_start_date(self):
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self