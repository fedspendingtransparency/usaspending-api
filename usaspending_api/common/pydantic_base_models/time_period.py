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
    def validate_date_are_after_fy2008(self):
        if self.start_date < '2007-10-01':
            raise ValueError("start_date cannot be earlier than '2007-10-01'")
        if self.end_date < '2007-10-01':
            raise ValueError("end_date cannot be earlier than '2007-10-01'")
        return self