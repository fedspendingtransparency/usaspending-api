from ninja import NinjaAPI

from usaspending_api.config import CONFIG
from usaspending_api.llm.v2.router import router as llm_router

api = NinjaAPI(
    title="USASpending API",
    version="1.0.0",
    urls_namespace="async_api",
    docs_url="/docs/new" if CONFIG.ENV_CODE.lower() != "prd" else None,
    openapi_url="/openapi.json" if CONFIG.ENV_CODE.lower() != "prd" else None,
)

api.add_router("/api/v2/llm/", llm_router, url_name_prefix="v2")
