from pydantic import BaseModel


class AwardAmount(BaseModel):
    lower_bound: int | None = None
    upper_bound: int | None = None
