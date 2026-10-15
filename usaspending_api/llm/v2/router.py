from ninja import Router

from usaspending_api.llm.v2.auth import LLMApiKeyAuth

router = Router(auth=LLMApiKeyAuth(), tags=["SmartAssist"])

# The router must be initialized before importing the view to avoid circular import
from usaspending_api.llm.v2.views import filter_search  # noqa: E402, F401
