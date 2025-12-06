from typing import Literal

from openapi_pydantic import Server, Response, MediaType, RequestBody
from openapi_pydantic.util import PydanticSchema
from pydantic import BaseModel, Field
from openapi_pydantic.v3 import OpenAPI, Info, PathItem, Operation

from usaspending_api.dynamic_docs.request_models.advanced_filter import AdvancedFilter
from usaspending_api.dynamic_docs.response_models.common import PageMetadataResponse


class _Result(BaseModel):
    award_amount: float = Field(..., alias="Award Amount", description="Amount for the award")
    award_id: int = Field(..., alias="Award ID", description="ID of the award")


class SpendingByAwardResponse(BaseModel):
    limit: int = Field(..., description="Number of results to return")
    messages: list[str] = Field(..., description="List of messages pertaining to the request, response, etc.")
    page_metadata: PageMetadataResponse
    results: list[_Result] = Field(..., description="List of results")
    spending_level: Literal["awards", "subawards"] = Field(..., description="Type of data that will be returned")


def spending_by_award_open_api_paths() -> dict[str, PathItem]:
    return {
        "/v2/search/spending_by_award": PathItem(
            post=Operation(
                operationId="searchAwards",
                description="Search for spending by award",
                requestBody=RequestBody(
                    content={"application/json": {"schema": PydanticSchema(schema_class=AdvancedFilter)}}
                ),
                responses={
                    "200": Response(
                        description="List of award spending",
                        content={"application/json": {"schema": PydanticSchema(schema_class=SpendingByAwardResponse)}},
                    )
                },
            )
        )
    }
