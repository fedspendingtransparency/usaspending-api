from ninja import NinjaAPI

from usaspending_api.config import CONFIG
from usaspending_api.llm.v2 import views as llm_views  # noqa: F401  (registers routes on llm_router)
from usaspending_api.llm.v2.router import router as llm_router

api = NinjaAPI(
    title="USAspending API",
    version="1.0.0",
    urls_namespace="async_api",
    docs_url=None if CONFIG.ENV_CODE.lower() == "prd" else "/docs/new",
    openapi_url=None if CONFIG.ENV_CODE.lower() == "prd" else "/openapi.json",
)

api.add_router("/api/v2/llm/", llm_router, url_name_prefix="v2")
