from pydantic import BaseModel


class NAICSCodeObject(BaseModel):
    require: list[str] | None = None
    exclude: list[str] | None = None


class PSCCodeObject(BaseModel):
    require: list[list[str]] | None = None
    exclude: list[list[str]] | None = None


class TASCodeObject(BaseModel):
    require: list[list[str]] | None = None
    exclude: list[list[str]] | None = None
