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


class TASCodeComponentObject(BaseModel):
    aid: str | None = None
    main: str | None = None
    ata: str | None = None
    bpoa: str | None = None
    epoa: str | None = None
    a: str | None = None
    sub: str | None = None
    agency: str | None = None
    faaid: str | None = None
    famain: str | None = None
