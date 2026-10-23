import hmac
import logging
import time
from typing import Optional

from django.http import HttpRequest
from ninja.security import APIKeyHeader

from usaspending_api.common.helpers.aws_helpers import async_aws_client

logger = logging.getLogger(__name__)

LLM_API_SECRET_NAME = "llm_api_secret"

# Reuse a fetched secret for this long before asking Secrets Manager again.
SECRET_CACHE_TTL_SECONDS = 300

# Floor between fetches that are not driven by the TTL (a failed fetch, or a key
# that did not match). This bounds how often bad keys can make us call AWS.
SECRET_REFETCH_FLOOR_SECONDS = 15


class LLMApiKeyAuth(APIKeyHeader):
    """Validates the X-LLM-API-Key header against the UUID stored in Secrets Manager."""

    param_name = "X-LLM-API-Key"

    # Process-wide cache of the secret. Each uwsgi worker keeps its own copy.
    _secret: Optional[str] = None
    _fetched_at: float = 0.0
    _last_attempt_at: float = 0.0

    async def authenticate(self, request: HttpRequest, key: str) -> Optional[str]:
        if not key:
            return None

        # Try the cached secret first. On a miss the secret may have been rotated, so
        # allow one refetch. That refetch is rate limited so a bad key can't hammer AWS.
        for force_refresh in (False, True):
            stored_uuid = await self._get_secret_uuid(force_refresh=force_refresh)
            if stored_uuid is not None and self._matches(key, stored_uuid):
                return key
        return None

    @staticmethod
    def _matches(key: str, stored_uuid: str) -> bool:
        return hmac.compare_digest(key.strip().encode("utf-8"), stored_uuid.strip().encode("utf-8"))

    @classmethod
    async def _get_secret_uuid(cls, force_refresh: bool = False) -> Optional[str]:
        now = time.monotonic()
        is_fresh = cls._secret is not None and (now - cls._fetched_at) < SECRET_CACHE_TTL_SECONDS
        if is_fresh and not force_refresh:
            return cls._secret

        if (now - cls._last_attempt_at) < SECRET_REFETCH_FLOOR_SECONDS:
            # Fetched (or tried to) very recently. Serve what we have, stale or not.
            return cls._secret

        cls._last_attempt_at = now
        secret = await cls._fetch_secret_from_aws()
        if secret is not None:
            cls._secret = secret
            cls._fetched_at = now
        elif cls._secret is not None:
            logger.warning(f"Could not refresh LLM API secret '{LLM_API_SECRET_NAME}'; using the cached value")
        return cls._secret

    @classmethod
    def reset_cache(cls) -> None:
        """Drop the cached secret. Mainly for tests."""
        cls._secret = None
        cls._fetched_at = 0.0
        cls._last_attempt_at = 0.0

    @staticmethod
    async def _fetch_secret_from_aws() -> Optional[str]:
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
