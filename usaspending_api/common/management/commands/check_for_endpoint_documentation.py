from django.core.management.base import BaseCommand
from usaspending_api.common.helpers.endpoint_documentation import (
    CURRENT_ENDPOINT_PREFIXES,
    get_endpoint_urls_doc_paths_and_docstrings,
    get_endpoints_from_endpoints_markdown,
    validate_docs,
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

        # By request, let's count how much documentation falls in the contracts directory vs not-contracts.
        contract_count = sum(
            getattr(
                e[1].callback.cls if getattr(e[1].callback, "cls", None) else e[1].callback, "endpoint_doc", ""
            ).startswith("usaspending_api/api_contracts/contracts/")
            for e in endpoints
        )

        print("Looks like endpoint documentation is happy, healthy, wealthy, and wise.  Current tally:")
        print("    contract count: {:,}".format(contract_count))
        print("    non-contract count: {:,}".format(len(endpoints) - contract_count))
        if (len(endpoints) - contract_count) == 0:
            print()
            print("*** GOOD JOB TEAM! ***")
