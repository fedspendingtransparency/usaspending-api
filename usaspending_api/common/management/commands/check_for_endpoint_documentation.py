import logging

from django.core.management.base import BaseCommand

from usaspending_api.common.helpers.endpoint_documentation import (
    _EXCLUDED_URLS,
    CURRENT_ENDPOINT_PREFIXES,
    get_endpoint_urls_doc_paths_and_docstrings,
    get_endpoints_from_endpoints_markdown,
    get_view_for_endpoint,
    validate_docs,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Checks endpoints to ensure they point to documentation and are contained in the master endpoint list."

    def handle(self, *args, **options) -> None:
        master_endpoint_list = get_endpoints_from_endpoints_markdown()

        endpoints = get_endpoint_urls_doc_paths_and_docstrings(CURRENT_ENDPOINT_PREFIXES)

        messages = []
        for endpoint in endpoints:
            messages.extend(validate_docs(*endpoint, master_endpoint_list))

        if messages:
            for message in messages:
                logger.error(message)
            exit(1)

        # By request, let's count how much documentation falls in the contracts directory vs not-contracts.
        contract_count = sum(
            getattr(get_view_for_endpoint(*e), "endpoint_doc", "").startswith(
                "usaspending_api/api_contracts/contracts/"
            )
            for e in endpoints
        )

        logger.info("Looks like endpoint documentation is happy, healthy, wealthy, and wise.  Current tally:")
        logger.info(f"    contract count: {contract_count:,}")
        logger.info(f"    non-contract count: {len(endpoints) - (contract_count + len(_EXCLUDED_URLS)):,}")
        logger.info(f"    excluded count: {len(_EXCLUDED_URLS):,}")
        if (len(endpoints) - contract_count) == 0:
            logger.info("\n*** GOOD JOB TEAM! ***")
