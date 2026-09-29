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


class GuardrailServiceUnavailable(Exception):
    """Raised when request moderation cannot be completed safely."""


@dataclass(frozen=True)
class GuardrailAssessment:
    action: str
    action_reason: str | None
    assessments: list[dict[str, Any]]
    coverage: dict[str, Any]
    usage: dict[str, Any]
    latency_ms: int | None

    @property
    def intervened(self) -> bool:
        return self.action == "GUARDRAIL_INTERVENED"


class BedrockGuardrailService:
    """
    Applies the configured Amazon Bedrock Guardrail to user-provided request text.

    Guardrail ID is a static deployment configuration.
    Guardrail Version is read from Secrets Manager and cached briefly so a published version can be changed without
    rebuilding/redeploying the application.
    """

    _version_lock = threading.Lock()
    _version_cache: str | None = None
    _version_cache_expires_at: float = 0.0

    def __init__(
        self,
        bedrock_runtime_client: Any | None = None,
        secrets_manager_client: Any | None = None,
    ) -> None:
        self.guardrail_id = settings.AWS_BEDROCK_GUARDRAIL_ID
        self.version_secret_arn = settings.AWS_BEDROCK_GUARDRAIL_VERSION_SECRET_ARN
        self.version_cache_seconds = settings.AWS_BEDROCK_GUARDRAIL_VERSION_CACHE_SECONDS
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
        """
        Submit input text for moderation by Guardrails.

        The source is "INPUT" because this endpoint is moderating user input before it is persisted or sent downstream.
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Guardrails input text must be a non-empty string.")

        self._validate_configuration()
        guardrail_version = self._get_guardrail_version()

        try:
            response = self.bedrock_runtime_client.apply_guardrail(
                guardrailIdentifier=self.guardrail_id,
                guardrailVersion=guardrail_version,
                source="INPUT",
                content=[
                    {
                        "text": {
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
            raise GuardrailServiceUnavailable("Unable to moderate reqeust content.") from exc

        assessment = GuardrailAssessment(
            action=response.get("action"),
            action_reason=response.get("actionReason"),
            assessments=response.get("assessments", []),
            coverage=response.get("guardrailCoverage", {}),
            usage=response.get("usage", {}),
            latency_ms=response.get("latencyMs"),
        )

        logger.info(
            "Bedrock Guardrail assessed filter-search request.",
            extra={
                "guardrail_id": self.guardrail_id,
                "guardrail_version": guardrail_version,
                "guardrail_action": assessment.action,
                "guardrail_action_reason": assessment.action_reason,
                "guardrail_assessments": assessment.assessments,
                "guardrail_coverage": assessment.coverage,
                "guardrail_usage": assessment.usage,
                "guardrail_latency_ms": assessment.latency_ms,
            },
        )

        return assessment

    def _validate_configuration(self) -> None:
        missing_settings = [
            setting_name
            for setting_name, value in (
                ("AWS_BEDROCK_GUARDRAIL_ID", self.guardrail_id),
                ("AWS_BEDROCK_GUARDRAIL_VERSION_SECRET_ARN", self.version_secret_arn),
            )
            if not value
        ]

        if missing_settings:
            logger.error(
                "Bedrock Guardrails configuration is incomplete.",
                extra={"missing_settings": missing_settings},
            )
            raise GuardrailConfigurationError("Bedrock Guardrails configuration is incomplete.")

    def _get_guardrail_version(self) -> str:
        now = time.monotonic()

        if self.__class__._cached_version is not None and now < self.__class__._version_cache_expires_at:
            return self.__class__._cached_version

        with self.__class__._version_lock:
            now = time.monotonic()

            if self.__class__._cached_version is not None and now < self.__class__._version_cache_expires_at:
                return self.__class__._cached_version

            try:
                response = self.secrets_manager_client.get_secret_value(SecretId=self.version_secret_arn)
                secret_string = response["SecretString"]
                secret_value = json.loads(secret_string)
                version = str(secret_value["version"]).strip()
            except (BotoCoreError, ClientError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                logger.exception(
                    "Unable to retreive Bedrock Guardrail version.",
                    extra={
                        "gaurdrail_id": self.guardrail_id,
                        "version_secret_arn": self.version_secret_arn,
                        "error_type": type(exc).__name__,
                    },
                )
                raise GuardrailConfigurationError("Unable to retrieve Bedrock Guardrail version.") from exc

            if not version:
                logger.error(
                    "Bedrock Guardrail version secret contains an empty version.",
                    extra={
                        "guardrail_id": self.guardrail_id,
                        "version_secret_arn": self.version_secret_arn,
                    },
                )
                raise GuardrailConfigurationError("Bedrock Guardrail version secret contains an empty version.")

            self.__class__._cached_version = version
            self.__class__._version_cache_expires_at = now + self.version_cache_seconds

            logger.info(
                "Refreshed Bedrock Guardrail version from Secrets Manager.",
                extra={
                    "guardrail_id": self.guardrail_id,
                    "guardrail_version": version,
                    "version_cache_seconds": self.version_cache_seconds,
                },
            )

            return version
