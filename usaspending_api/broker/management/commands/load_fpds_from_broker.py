from django.core.management.base import BaseCommand

from usaspending_api.data_load.fpds_loader import run_fpds_load


class Command(BaseCommand):
    help = "Sync USAspending DB FPDS data using Broker for new or modified records and S3 for deleted IDs"

    def handle(self, *args, **options):
        run_fpds_load([24066416, 24066963, 24067231])
