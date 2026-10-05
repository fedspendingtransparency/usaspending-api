from ninja import Router

from usaspending_api.llm.v2.auth import LLMApiKeyAuth

router = Router(auth=LLMApiKeyAuth(), tags=["SmartAssist"])
