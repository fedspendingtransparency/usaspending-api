from pydantic import BaseModel, model_validator
from typing_extensions import Self


class AwardAmount(BaseModel):
    lower_bound: int | None = None
    upper_bound: int | None = None

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.lower_bound is not None and self.upper_bound is not None:
            if self.upper_bound < self.lower_bound:
                raise ValueError("upper_bound must be greater than or equal to lower_bound") from None
        return self
