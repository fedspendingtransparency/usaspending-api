import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand

logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Load LLM fixture data for AI models, prompts, and assistants."

    def handle(self, *args, **options) -> None:
        logger.info("Loading AI models...")
        call_command("loaddata", "usaspending_api/llm/fixtures/ai_models.yaml")

        logger.info("Loading prompts...")
        call_command("loaddata", "usaspending_api/llm/fixtures/prompts.yaml")

        logger.info("Loading assistants...")
        call_command("loaddata", "usaspending_api/llm/fixtures/assistants.yaml")

        logger.info("Successfully loaded LLM fixtures")
