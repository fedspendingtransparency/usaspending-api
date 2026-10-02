import json
import logging
import threading
import time
from dataclasses import dataclass
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

    Guardrail ID is a static deployment configuration.
    Guardrail Tag is read from Secrets Manager and cached briefly so a published tag can be changed without
    rebuilding/redeploying the application.
    """

    _tag_cache: str | None = None
    _tag_cache_expires_at: float = 0.0
    # Use a lock to ensure thread-safe access to the cache.
    _tag_lock = threading.Lock()

    def __init__(
        self,
        bedrock_runtime_client: Any | None = None,
        secrets_manager_client: Any | None = None,
    ) -> None:
        # Guardrail Identifier (ID or ARN).
        self.guardrail_id = settings.AWS_BEDROCK_GUARDRAIL_ID
        # Secrets Manager ARN for the Guardrail tag.
        self.tag_secret_arn = settings.AWS_BEDROCK_GUARDRAIL_TAG_SECRETS_MGR_ARN
        # Cache duration (default: 5 minutes).
        self.tag_cache_seconds = settings.AWS_BEDROCK_GUARDRAIL_TAG_CACHE_SECONDS
        # AWS Region (default: same as USAspending AWS Region).
        self.region_name = settings.AWS_BEDROCK_GUARDRAIL_AWS_REGION

        self.bedrock_runtime_client = bedrock_runtime_client or boto3.client(
            "bedrock-runtime",
            region_name=self.region_name,
        )
        self.secrets_manager_client = secrets_manager_client or boto3.client(
            "secretsmanager",
            region_name=self.region_name,
        )

    def assess_input(self, text: str) -> GuardrailAssessment:
        """Submit input text for moderation by Guardrails."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Guardrails input text must be a non-empty string.")

        self._validate_configuration()
        guardrail_tag = self._get_guardrail_tag()

        try:
            # Docs: https://docs.aws.amazon.com/boto3/latest/reference/services/bedrock-runtime/client/apply_guardrail.html
            response = self.bedrock_runtime_client.apply_guardrail(
                # Guardrail ID or ARN.
                guardrailIdentifier=self.guardrail_id,
                # Guardrail version number or tag.
                guardrailVersion=guardrail_tag,
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
                    "guardrail_tag": self._get_guardrail_tag(),
                    "error_type": type(exc).__name__,
                },
            )
            raise GuardrailServiceUnavailable("Unable to moderate reqeust content.") from exc

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
                "guardrail_tag": guardrail_tag,
                "guardrail_action": assessment.action,
                "guardrail_action_reason": assessment.action_reason,
                "guardrail_assessments": assessment.assessments,
                "guardrail_usage": assessment.usage,
                "guardrail_coverage": assessment.guardrail_coverage,
            },
        )

        return assessment

    def _validate_configuration(self) -> None:
        missing_settings = [
            setting_name
            for setting_name, value in (
                ("AWS_BEDROCK_GUARDRAIL_ID", self.guardrail_id),
                ("AWS_BEDROCK_GUARDRAIL_TAG_SECRETS_MGR_ARN", self.tag_secret_arn),
            )
            if not value
        ]

        if missing_settings:
            logger.error(
                "Bedrock Guardrails configuration is incomplete.",
                extra={"missing_settings": missing_settings},
            )
            raise GuardrailConfigurationError("Bedrock Guardrails configuration is incomplete.")

    def _get_guardrail_tag(self) -> str:
        now = time.monotonic()

        if self.__class__._tag_cache is not None and now < self.__class__._tag_cache_expires_at:
            return self.__class__._tag_cache

        with self.__class__._tag_lock:
            now = time.monotonic()

            if self.__class__._tag_cache is not None and now < self.__class__._tag_cache_expires_at:
                return self.__class__._tag_cache

            try:
                # Docs: https://docs.aws.amazon.com/boto3/latest/reference/services/secretsmanager/client/get_secret_value.html
                response = self.secrets_manager_client.get_secret_value(SecretId=self.tag_secret_arn)
                secret_string = response["SecretString"]
                secret_value = json.loads(secret_string)
                tag = str(secret_value["tag"]).strip()
            except (BotoCoreError, ClientError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                logger.exception(
                    "Unable to retreive Bedrock Guardrail tag.",
                    extra={
                        "gaurdrail_id": self.guardrail_id,
                        "tag_secret_arn": self.tag_secret_arn,
                        "error_type": type(exc).__name__,
                    },
                )
                raise GuardrailConfigurationError("Unable to retrieve Bedrock Guardrail tag.") from exc

            if not tag:
                logger.error(
                    "Bedrock Guardrail tag secret contains an empty tag.",
                    extra={
                        "guardrail_id": self.guardrail_id,
                        "tag_secret_arn": self.tag_secret_arn,
                    },
                )
                raise GuardrailConfigurationError("Bedrock Guardrail tag secret contains an empty tag.")

            self.__class__._tag_cache = tag
            self.__class__._tag_cache_expires_at = now + self.tag_cache_seconds

            logger.info(
                "Refreshed Bedrock Guardrail tag from Secrets Manager.",
                extra={
                    "guardrail_id": self.guardrail_id,
                    "guardrail_tag": tag,
                    "tag_cache_seconds": self.tag_cache_seconds,
                },
            )

            return tag
