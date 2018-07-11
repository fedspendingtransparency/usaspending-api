from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.common.helpers.etl_helpers import (update_assistance_c_to_d_linkages,
                                                        update_contract_c_to_d_linkages)


class Command(BaseCommand):

    def handle(self, *args, **options):

        with transaction.atomic():
            update_contract_c_to_d_linkages()
            update_assistance_c_to_d_linkages()
