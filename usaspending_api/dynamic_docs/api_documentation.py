import yaml
from openapi_pydantic.util import construct_open_api_with_schema_class
from openapi_pydantic.v3 import OpenAPI, Info, Server

from usaspending_api.dynamic_docs.response_models.v2.search.spending_by_award import spending_by_award_open_api_paths


def construct_base_open_api() -> OpenAPI:
    return OpenAPI(
        openapi="3.1.0",
        info=Info(title="USAspending API", version="0.0.0"),
        servers=[
            Server(
                url="https://api.usaspending.gov/api",
                description=(
                    "The USAspending API (Application Programming Interface) allows the public to access comprehensive U.S."
                    " government spending data."
                ),
            )
        ],
        paths={
            **spending_by_award_open_api_paths(),
        },
    )


open_api = construct_base_open_api()
open_api = construct_open_api_with_schema_class(open_api)

if __name__ == "__main__":
    # Need to add module to PYTHONPATH: `export PYTHONPATH="${PYTHONPATH}:${PWD}/usaspending_api/dynamic_docs"`
    with open("openapi.yaml", "w") as file:
        file.write(
            yaml.dump(
                open_api.model_dump(
                    by_alias=True,
                    mode="json",
                    exclude_none=True,
                    exclude_unset=True,
                ),
                sort_keys=False,
            )
        )
