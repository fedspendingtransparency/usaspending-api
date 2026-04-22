from pydantic import BaseModel, model_validator
from pydantic_core import PydanticCustomError
from typing_extensions import Self


class NAICSCodeObject(BaseModel):
    require: list[str | int] | None = None
    exclude: list[str | int] | None = None

    @model_validator(mode="after")
    def check_at_least_one_key(self) -> Self:
        if self.require is None and self.exclude is None:
            raise PydanticCustomError(
                "missing_required_field",
                'At least one of "require" or "exclude" must be provided',
                {"expected_type": "array, object"},
            )
        return self


class PSCCodeObject(BaseModel):
    require: list[list[str]] | None = None
    exclude: list[list[str]] | None = None

    @model_validator(mode="after")
    def check_at_least_one_key(self) -> Self:
        if self.require is None and self.exclude is None:
            raise PydanticCustomError(
                "missing_required_field",
                'At least one of "require" or "exclude" must be provided',
                {"expected_type": "array, object"},
            )
        return self


class TASCodeObject(BaseModel):
    require: list[list[str]] | None = None
    exclude: list[list[str]] | None = None

    @model_validator(mode="after")
    def check_at_least_one_key(self) -> Self:
        if self.require is None and self.exclude is None:
            raise PydanticCustomError(
                "missing_required_field",
                'At least one of "require" or "exclude" must be provided',
                {"expected_type": "array, object"}
            )
        return self
