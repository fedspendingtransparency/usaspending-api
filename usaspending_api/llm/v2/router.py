import logging

from ninja import Router

from usaspending_api.llm.v2.auth import LLMApiKeyAuth

logger = logging.getLogger(__name__)

router = Router(auth=LLMApiKeyAuth(), tags=["SmartAssist"])
