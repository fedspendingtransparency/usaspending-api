from django.core.management.base import BaseCommand

from usaspending_api.transactions.transfer_records_base import BaseTransferClass
from usaspending_api.transactions.models.source_assistance_transaction import SourceAssistanceTransaction


class Command(BaseTransferClass, BaseCommand):
    help = "Upsert assistance transactions from a Broker database into an USAspending database"
    broker_select_sql = "SELECT {} FROM {} WHERE is_active IS true"
    broker_source_table_name = SourceAssistanceTransaction().broker_source_table
    destination_table_name = SourceAssistanceTransaction().table_name
    last_load_record = "source_assistance_transaction"
    lookback_minutes = 15
    shared_pk = "published_award_financial_assistance_id"
    working_file_prefix = "assistance_load_ids"
