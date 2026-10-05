import logging
from typing import Optional

from django.http import HttpRequest
from ninja.security import APIKeyHeader

from usaspending_api.common.helpers.aws_helpers import async_aws_client

logger = logging.getLogger(__name__)

LLM_API_SECRET_NAME = "llm_api_secret"


class LLMApiKeyAuth(APIKeyHeader):
    """Validates the X-LLM-API-Key header."""

    param_name = "X-LLM-API-Key"

    async def authenticate(self, request: HttpRequest, key: str) -> Optional[str]:
        if not key:
            return None

        stored_uuid = await self._get_secret_uuid()
        if stored_uuid is None:
            return None

        return key if key.strip() == stored_uuid.strip() else None

    @staticmethod
    async def _get_secret_uuid() -> Optional[str]:
        async with async_aws_client("secretsmanager") as client:
            try:
                response = await client.get_secret_value(SecretId=LLM_API_SECRET_NAME)
            except Exception as e:
                logger.error(f"Error retrieving LLM API secret '{LLM_API_SECRET_NAME}': {e}", exc_info=True)
                return None

        secret_string = response.get("SecretString")
        if not secret_string:
            logger.error(f"LLM API secret '{LLM_API_SECRET_NAME}' has no SecretString value")
            return None
        return secret_string
