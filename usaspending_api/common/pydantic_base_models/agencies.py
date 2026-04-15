from typing import Literal

from pydantic import BaseModel


class AgencyObject(BaseModel):
    type: Literal["awarding", "funding"]
    tier: Literal["toptier", "subtier"]
    name: str
    toptier_name: str | None = None
