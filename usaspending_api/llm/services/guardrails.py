import logging
import threading
import time
from dataclasses import dataclass
from functools import cached_property
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings

logger = logging.getLogger(__name__)


class GuardrailConfigurationError(Exception):
    """Raised when the Guardrails integration is not configured correctly."""

    pass


class GuardrailServiceUnavailable(Exception):
    """Raised when request moderation cannot be completed safely."""

    pass


@dataclass(frozen=True)
class GuardrailAssessment:
    action: str
    action_reason: str | None
    assessments: list[dict[str, Any]]
    usage: dict[str, Any]
    outputs: list[dict[str, Any]]
    guardrail_coverage: dict[str, Any]

    @property
    def intervened(self) -> bool:
        return self.action == "GUARDRAIL_INTERVENED"


class BedrockGuardrailService:
    """
    Applies the configured Amazon Bedrock Guardrail to user-provided request text.

    Guardrail ID is an environment variable added to deployment configuration.
    """

    @cached_property
    def client(self) -> Any:
        return boto3.client("bedrock-runtime", region_name=self.region_name)

    _version_cache: str | None = None
    _version_cache_expires_at: float = 0.0
    # Use a lock to ensure thread-safe access to the cache.
    _version_lock = threading.Lock()

    def __init__(self, bedrock_runtime_client: Any | None = None) -> None:
        # Guardrail Identifier (ID or ARN).
        self.guardrail_id = settings.AWS_BEDROCK_GUARDRAIL_ID
        # Cache duration (default: 5 minutes).
        self.version_cache_seconds = settings.AWS_BEDROCK_GUARDRAIL_VERSION_CACHE_SECONDS
        # AWS Region (default: same as USAspending AWS Region).
        self.region_name = settings.AWS_BEDROCK_GUARDRAIL_AWS_REGION

        self.bedrock_runtime_client = bedrock_runtime_client or self.client()

    def assess_input(self, text: str) -> GuardrailAssessment:
        """Submit input text for moderation by Guardrails."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Guardrails input text must be a non-empty string.")

        guardrail_version = self._get_guardrail_version()

        try:
            response = self.bedrock_runtime_client.apply_guardrail(
                # Guardrail ID or ARN.
                guardrailIdentifier=self.guardrail_id,
                # Guardrail version number or version.
                guardrailVersion=guardrail_version,
                # Source of the content (INPUT or OUTPUT).
                source="INPUT",
                # Content to moderate (can pass multiple content blocks).
                content=[
                    {
                        "text": {
                            # The query string to evaluate that was input by a user.
                            "text": text,
                        }
                    }
                ],
            )
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "Unable to apply Bedrock Guardrail to filter-search request.",
                extra={
                    "guardrail_id": self.guardrail_id,
                    "guardrail_version": self._get_guardrail_version(),
                    "error_type": type(exc).__name__,
                },
            )
            raise GuardrailServiceUnavailable("Unable to moderate request content.") from exc

        assessment = GuardrailAssessment(
            action=response.get("action"),
            action_reason=response.get("actionReason"),
            assessments=response.get("assessments", []),
            usage=response.get("usage", {}),
            outputs=response.get("outputs", []),
            guardrail_coverage=response.get("guardrailCoverage", {}),
        )

        logger.info(
            "Bedrock Guardrail assessed filter-search request.",
            extra={
                "guardrail_id": self.guardrail_id,
                "guardrail_version": guardrail_version,
                "guardrail_action": assessment.action,
                "guardrail_action_reason": assessment.action_reason,
                "guardrail_assessments": assessment.assessments,
                "guardrail_usage": assessment.usage,
                "guardrail_coverage": assessment.guardrail_coverage,
            },
        )

        return assessment

    def _get_guardrail_version(self) -> str:
        now = time.monotonic()

        if self.__class__._version_cache is not None and now < self.__class__._version_cache_expires_at:
            return self.__class__._version_cache

        with self.__class__._version_lock:
            now = time.monotonic()

            if self.__class__._version_cache is not None and now < self.__class__._version_cache_expires_at:
                return self.__class__._version_cache

            try:
                response = self.bedrock_runtime_client.list_guardrails(guardrailIdentifier=self.guardrail_id)
                numeric_versions = [
                    # Retrieve a collection of Guardrail versions (as integers, for sorting).
                    int(item["version"])
                    for item in response.get("guardrails", [])
                    if item["version"].isdigit()
                ]

                if not numeric_versions:
                    # If no version number is set, use the default "DRAFT" version.
                    logger.warning(f"No published versions found for {self.guardrail_id}. Defaulting to 'DRAFT'.")
                    version = "DRAFT"
                else:
                    # Select the largest version number (i.e., most recent) and cast to str.
                    version = str(max(numeric_versions))
            except (BotoCoreError, ClientError, TypeError, ValueError) as exc:
                logger.exception(
                    "Unable to retrieve Bedrock Guardrail version.",
                    extra={
                        "guardrail_id": self.guardrail_id,
                        "error_type": type(exc).__name__,
                    },
                )
                raise GuardrailConfigurationError("Unable to retrieve Bedrock Guardrail version.") from exc

            self.__class__._version_cache = version
            self.__class__._version_cache_expires_at = now + self.version_cache_seconds

            return version
