from pydantic import BaseModel, Field


class PageMetadataResponse(BaseModel):
    hasNext: bool = Field(..., description="Whether another page exists")
    last_record_sort_value: str = Field(..., description="Provided in the request when filtering past 50,000 records")
    last_record_unique_id: int = Field(..., description="Provided in the request when filtering past 50,000 records")
    page: int = Field(..., description="Page number")
