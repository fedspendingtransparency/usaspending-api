from pydantic import BaseModel, field_validator, model_validator
from typing_extensions import Self


class StandardLocationObject(BaseModel):
    country: str
    state: str | None = None
    county: str | None = None
    city: str | None = None
    district_original: str | None = None
    district_current: str | None = None
    zip: str | None = None

    @field_validator('country')
    @classmethod
    def validate_country(cls, v: str) -> str:
        if len(v) != 3:
            raise ValueError("Country code must be exactly 3 characters")
        return v.upper()

    @field_validator('state')
    @classmethod
    def validate_state(cls, v: str) -> str | None:
        if v is not None and len(v) != 2:
            raise ValueError("State code must be exactly 2 characters")
        return v.upper() if v else None

    @field_validator('county')
    @classmethod
    def validate_county(cls, v: str) -> str | None:
        if v is not None and len(v) != 3:
            raise ValueError("County code must be exactly 3 digits")
        return v

    @field_validator('district_original', 'district_current')
    @classmethod
    def validate_district(cls, v: str) -> str | None:
        if v is not None and len(v) != 2:
            raise ValueError("District code must be exactly 2 characters")
        return v

    @field_validator('zip')
    @classmethod
    def validate_zip(cls, v: str) -> str | None:
        if v is not None and len(v) != 5:
            raise ValueError("ZIP code must be exactly 5 digits")
        return v

    @model_validator(mode='after')
    def validate_location_rules(self) -> Self:  # noqa: PLR0912
        # If county is provided, state must be provided
        if self.county is not None and self.state is None:
            raise ValueError("When 'county' is provided, 'state' must also be provided")

        # County cannot coexist with district_original or district_current
        if self.county is not None:
            if self.district_original is not None:
                raise ValueError("'county' and 'district_original' cannot both be provided")
            if self.district_current is not None:
                raise ValueError("'county' and 'district_current' cannot both be provided")

        # district_original requires state, country must be USA, but cannot coexist with district_current
        if self.district_original is not None:
            if self.state is None:
                raise ValueError("When 'district_original' is provided, 'state' must also be provided")
            if self.country.upper() != "USA":
                raise ValueError("When 'district_original' is provided, 'country' must be 'USA'")
            if self.district_current is not None:
                raise ValueError("'district_original' and 'district_current' cannot both be provided")

        # district_current requires state, country must be USA, but cannot coexist with district_original
        if self.district_current is not None:
            if self.state is None:
                raise ValueError("When 'district_current' is provided, 'state' must also be provided")
            if self.country.upper() != "USA":
                raise ValueError("When 'district_current' is provided, 'country' must be 'USA'")

        # city requires state and country or country
        if self.city is not None:
            if self.state is None and self.country is None:
                raise ValueError("When 'city' is provided, 'state' AND 'country' or 'country' must be provided")

        return self
