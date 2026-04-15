from typing import Literal

from pydantic import BaseModel


class TimePeriod(BaseModel):
    start_date: str
    end_date: str
    date_type: Literal["action_date", "date_signed", "last_modified_date", "new_awards_only"]