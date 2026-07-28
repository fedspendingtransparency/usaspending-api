from pydantic import BaseModel, Field, field_validator


class AutocompleteRequest(BaseModel):
    search_text: str = Field(..., min_length=1, description="Text to search for")
    limit: int = Field(default=10, ge=1, le=500, description="Number of results to return")

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Limit request parameter is not a valid, positive integer")
        return v

    @field_validator("search_text")
    @classmethod
    def validate_search_text(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Missing one or more required request parameters: search_text")
        return v.strip()
