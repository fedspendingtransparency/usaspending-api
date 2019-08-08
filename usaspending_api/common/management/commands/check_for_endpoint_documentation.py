from django.core.management.base import BaseCommand
from usaspending_api.common.helpers.endpoint_documentation import (
    CURRENT_ENDPOINT_PREFIXES, get_endpoint_urls_doc_paths_and_docstrings, get_endpoints_from_endpoints_markdown,
    validate_docs
)


class Command(BaseCommand):

    help = "Checks endpoints to ensure they point to documentation and are contained in the master endpoint list."

    def handle(self, *args, **options):
        master_endpoint_list = get_endpoints_from_endpoints_markdown()

        endpoints = get_endpoint_urls_doc_paths_and_docstrings(CURRENT_ENDPOINT_PREFIXES)

        messages = []
        for endpoint in endpoints:
            messages.extend(validate_docs(*endpoint, master_endpoint_list))

        if messages:
            for message in messages:
                print(message)
            exit(1)

        print("Looks like endpoint documentation is happy, healthy, wealthy, and wise")
