"""AWS Bedrock Guardrail definition and management for the filter-search assistant.

This module owns the guardrail's identity and content policy. AWS is the source of
truth: the guardrail is resolved by name via the Bedrock control plane and created
on demand if it does not yet exist (see `GuardrailManager`, added in a later step).

The constants below are intentionally the only place the guardrail's name and policy
are defined, so the guardrail can be tuned by editing this file.
"""

import logging
import time
from functools import cached_property, lru_cache
from typing import Any

import boto3
from botocore.config import Config

from usaspending_api.config import CONFIG

logger = logging.getLogger(__name__)

GUARDRAIL_VERSION = "DRAFT"
GUARDRAIL_NAME = "usaspending-filter-search"
GUARDRAIL_DESCRIPTION = "Content safety guardrail for the USAspending filter-search assistant."

CONTENT_POLICY_CONFIG = {
    "filtersConfig": [
        {"type": "SEXUAL", "inputStrength": "HIGH", "outputStrength": "HIGH"},
        {"type": "VIOLENCE", "inputStrength": "MEDIUM", "outputStrength": "HIGH"},
        {"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
        {"type": "INSULTS", "inputStrength": "MEDIUM", "outputStrength": "MEDIUM"},
        {"type": "MISCONDUCT", "inputStrength": "HIGH", "outputStrength": "HIGH"},
        {"type": "PROMPT_ATTACK", "inputStrength": "HIGH", "outputStrength": "NONE"},
    ]
}

# Message returned to the caller when a user's input is blocked by the guardrail.
BLOCKED_INPUT_MESSAGING = (
    "This request can't be processed. The filter-search assistant only helps with "
    "searching federal spending data on USAspending.gov."
)

# Message returned to the caller when the model's output is blocked by the guardrail.
BLOCKED_OUTPUT_MESSAGING = (
    "This response was withheld because it didn't meet content safety requirements. "
    "Please rephrase your request about federal spending data."
)


class GuardrailManager:
    """Resolves the filter-search guardrail via the Bedrock control plane, creating it on demand.

    AWS is the source of truth. `ensure_guardrail` looks the guardrail up by name and, if it does not
    exist, creates it from the constants in this module and waits until it is READY. The returned dict is
    the `guardrailConfig` shape expected by the Bedrock runtime `converse` API.
    """

    # Bounded wait for a freshly created guardrail to leave CREATING and reach READY before first use.
    READY_POLL_MAX_ATTEMPTS = 30
    READY_POLL_INTERVAL_SECONDS = 2

    @cached_property
    def client(self) -> Any:
        """Lazy-load the Bedrock *control-plane* client used for guardrail management."""
        config = Config(retries={"max_attempts": 3, "mode": "adaptive"})
        return boto3.client(service_name="bedrock", region_name=CONFIG.AWS_REGION, config=config)

    def _find_guardrail_id(self) -> str | None:
        """Return the id of the guardrail named `GUARDRAIL_NAME`, or `None` if it does not exist."""
        paginator = self.client.get_paginator("list_guardrails")
        for page in paginator.paginate():
            for summary in page.get("guardrails", []):
                if summary.get("name") == GUARDRAIL_NAME:
                    return summary["id"]
        return None

    def _wait_until_ready(self, guardrail_id: str) -> None:
        """Poll `get_guardrail` until the guardrail is READY, so it is not used before it is usable.

        Raises:
            RuntimeError: if the guardrail reports a FAILED status.
            TimeoutError: if it does not reach READY within the bounded number of attempts.
        """
        for _ in range(self.READY_POLL_MAX_ATTEMPTS):
            status = self.client.get_guardrail(guardrailIdentifier=guardrail_id, guardrailVersion=GUARDRAIL_VERSION)[
                "status"
            ]
            if status == "READY":
                return
            if status == "FAILED":
                raise RuntimeError(f"Guardrail '{GUARDRAIL_NAME}' (id={guardrail_id}) entered FAILED status.")
            time.sleep(self.READY_POLL_INTERVAL_SECONDS)
        timeout = self.READY_POLL_MAX_ATTEMPTS * self.READY_POLL_INTERVAL_SECONDS
        raise TimeoutError(f"Guardrail '{GUARDRAIL_NAME}' (id={guardrail_id}) did not become READY within {timeout}s.")

    def _create_guardrail(self) -> str:
        """Create the guardrail from this module's constants and wait until it is READY.

        Returns:
            The id of the newly created guardrail.
        """
        response = self.client.create_guardrail(
            name=GUARDRAIL_NAME,
            description=GUARDRAIL_DESCRIPTION,
            contentPolicyConfig=CONTENT_POLICY_CONFIG,
            blockedInputMessaging=BLOCKED_INPUT_MESSAGING,
            blockedOutputMessaging=BLOCKED_OUTPUT_MESSAGING,
        )
        guardrail_id = response["guardrailId"]
        self._wait_until_ready(guardrail_id)
        return guardrail_id

    def ensure_guardrail(self) -> dict:
        """Get-or-create the guardrail and return the `converse` `guardrailConfig` dict.

        Returns:
            `{"guardrailIdentifier": <id>, "guardrailVersion": GUARDRAIL_VERSION}`.
        """
        guardrail_id = self._find_guardrail_id()
        if guardrail_id is None:
            logger.info("Guardrail '%s' not found; creating it.", GUARDRAIL_NAME)
            guardrail_id = self._create_guardrail()
            logger.info("Created guardrail '%s' (id=%s).", GUARDRAIL_NAME, guardrail_id)
        else:
            logger.info("Using existing guardrail '%s' (id=%s).", GUARDRAIL_NAME, guardrail_id)
        return {"guardrailIdentifier": guardrail_id, "guardrailVersion": GUARDRAIL_VERSION}


@lru_cache(maxsize=1)
def get_guardrail_config() -> dict:
    """Return the guardrail's `converse` config, resolving (and creating) the guardrail once per process.
    """
    return GuardrailManager().ensure_guardrail()
