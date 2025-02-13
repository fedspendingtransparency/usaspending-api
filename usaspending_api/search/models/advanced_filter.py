from typing import List, Literal, Optional

from pydantic import BaseModel


class TimePeriod(BaseModel):
    start_date: str
    end_date: str
    date_type: Optional[Literal["action_date", "date_signed", "last_modified_date", "new_awards_only"]]


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
