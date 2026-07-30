from typing import Any

from pydantic import BaseModel, Field, field_validator


class FilterHashRequest(BaseModel):
    """
    Generic request model for filter hash endpoint.

    This model intentionally accepts any JSON structure for backward compatibility.
    The endpoint acts as a hash/storage service and does not validate the internal
    structure of the filters object. The 'filters' field can be:
    - A dictionary with any structure (e.g., advanced search filters)
    - A string (e.g., simple search term)
    - null/None

    Additional fields beyond 'filters' and 'version' are also accepted.
    """
    filters: dict[str, Any] | str | None = Field(
        default=None,
        description="Filter criteria for advanced search. Can be a dictionary, string, or null."
    )
    version: str | None = Field(
        default=None,
        description="API version string (e.g., '2019-07-26')."
    )

    class Config:
        extra = "allow"

    @field_validator("filters", mode="before")
    @classmethod
    def validate_filters(cls, v: Any) -> Any:
        return v


class HashLookupRequest(BaseModel):
    hash: str = Field(
        ...,
        description="MD5 hash of the filter to retrieve.",
        min_length=32,
        max_length=32
    )

    class Config:
        extra = "forbid"

    @field_validator("hash")
    @classmethod
    def validate_hash_format(cls, v: str) -> str:
        if not all(char in "0123456789abcdef" for char in v.lower()):
            raise ValueError("Hash must be a valid hexadecimal string.")
        return v.lower()
