from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages


class Command(BaseCommand):

    LINKAGE_TYPES = ['contract', 'assistance']

    def handle(self, *args, **options):

        with transaction.atomic():
            for link_type in self.LINKAGE_TYPES:
                update_c_to_d_linkages(type=link_type)
