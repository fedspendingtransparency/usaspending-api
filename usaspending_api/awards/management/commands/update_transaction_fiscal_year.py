from usaspending_api.awards.models import TransactionNormalized
from django.core.management.base import BaseCommand
import logging


class Command(BaseCommand):
    help = "Updates the fiscal year for all transactions based on their individual action dates"

    logger = logging.getLogger("script")

    def handle(self, *args, **options):
        all_transactions = TransactionNormalized.objects.all()
        for transaction in all_transactions:
            # save by default has been overridden to auto-set the fiscal_year based on the action_date
            transaction.save()
