from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic.json_schema import models_json_schema


class TimePeriod(BaseModel):
    """
    A date range to use when searching for records.

    Awards filter to records that overlap the date range since awards are themselves a range of dates.
    Subawards and transactions filter to records that fall within the range since they are a single point in time.

    Values should be formatted as yyyy-MM-dd (e.g., 2025-01-31).
    """

    start_date: str = Field(..., title="Start Date", description="Earliest date of the range", examples=["2024-10-01"])
    end_date: str = Field(..., title="End Date", description="Latest date of the range", examples=["2025-09-30"])
    date_type: Optional[Literal["action_date", "date_signed", "last_modified_date", "new_awards_only"]] = Field(
        None, title="Date Type", description="Date fields that can be filtered", examples=["action_date"]
    )


class Agency(BaseModel):
    type: Literal["awarding", "funding"]
    tier: Literal["toptier", "subtier"]
    name: str
    toptier_name: Optional[str]


class Location(BaseModel):
    country: str
    state: Optional[str]
    county: Optional[str]
    city: Optional[str]
    district_original: Optional[str]
    district_current: Optional[str]
    zip: Optional[str]


class AdvancedFilter(BaseModel):
    keywords: Optional[List[str]]
    description: Optional[str]
    time_period: Optional[List[TimePeriod]]
    place_of_performance: Optional[Literal["domestic", "foreign"]]
    agencies: Optional[List[Agency]]
    recipient_search_text: Optional[List[str]]
    recipient_scope: Optional[Literal["domestic", "foreign"]]
    recipient_locations: Optional[List[Location]]


def print_json_schema(
    models: list[type[BaseModel]], json_schema_mode: Literal["serialization", "validation"] = "validation"
) -> None:
    _, schemas = models_json_schema(
        [(model, json_schema_mode) for model in models], ref_template="#/components/schemas/{model}"
    )
    openapi_schema = {
        "openapi": "3.1.0",
        "info": {"title": "USAspending API", "version": "0.0.0"},
        "components": {"schemas": schemas.get("$defs")},
    }
    print(yaml.dump(openapi_schema, sort_keys=False))


if __name__ == "__main__":
    print_json_schema([AdvancedFilter, Location, Agency, TimePeriod])
