from typing import Any, Callable, Literal

from pydantic import BaseModel, Field


class AIToolDescription(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]


class AITool(BaseModel):
    description: AIToolDescription
    function: Callable
    logging: Callable = lambda tool_use: print(f"Tool: {tool_use.name} with {tool_use.input}")


class RecipientFilter(BaseModel):
    recipient_search_text: list[str] = Field(
        default_factory=list,
        description="Recipient search values (name, uei, duns)",
        min_length=1,
    )


class RecipientDisplay(BaseModel):
    """Model for recipient display info"""
    entity: Literal[
        "Recipient",
        "Parent recipient",
        "Child recipient",
        "Subcontractor",
    ] = Field(description="The type of recipient entity")
    standalone: str = Field(description="Short recipient name for filter chips (e.g., 'ACME CORP', 'John Smith')")
    title: str = Field(description="Full recipient name for display")


class SelectedRecipient(BaseModel):
    """Model for a selected entity"""

    identifier: str = Field(
        description=(
            "Unique identifier for the recipient. "
            "Format varies by type: UEI for entities, DUNS for legacy, or internal ID"
        )
    )
    filter: RecipientFilter
    display: RecipientDisplay
