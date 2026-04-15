from pydantic import BaseModel, model_validator


class ProgramActivityObject(BaseModel):
    name: str | None = None
    code: str | None = None

    @model_validator(mode='after')
    def check_at_least_one_field(self):
        """At least one of the following fields are required when using the ProgramActivityObject in an API request"""

        if self.name is None and self.code is None:
            raise ValueError("At least one of 'name' or 'code' must be provided")
        return self
