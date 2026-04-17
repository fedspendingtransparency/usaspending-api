from typing import Any, Callable

from pydantic import BaseModel, Field


class AIToolDescription(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]


class AITool(BaseModel):
    description: AIToolDescription
    function: Callable
    logging: Callable = lambda tool_use: print(f"Tool: {tool_use.name} with {tool_use.input}")


class Location(BaseModel):
    country: str
    state: str | None = Field(default=None)
    county: str | None = Field(default=None)
    city: str | None = Field(default=None)
    district_original: str | None = Field(
        default=None, description="The congressional district at the time of the contract date."
    )
    district_current: str | None = Field(default=None, description="The current congressional district.")
    zip_code: str | None = Field(default=None)


class AdvancedSearchFilter(BaseModel):
    keywords: list[str] | None = Field(default=None)
    place_of_performance_locations: list[Location] | None = Field(default=None)
