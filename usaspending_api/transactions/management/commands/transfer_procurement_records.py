from django.core.management.base import BaseCommand

from usaspending_api.transactions.transfer_records_base import BaseTransferClass
from usaspending_api.transactions.models.source_procurement_transaction import SourceProcurmentTransaction


class Command(BaseTransferClass, BaseCommand):
    help = "Upsert procurement transactions from a Broker database into an USAspending database"
    broker_select_sql = "SELECT {} FROM {}"
    broker_source_table_name = SourceProcurmentTransaction().broker_source_table
    destination_table_name = SourceProcurmentTransaction().table_name
    last_load_record = "source_procurement_transaction"
    lookback_minutes = 0
    shared_pk = "detached_award_procurement_id"
    working_file_prefix = "procurement_load_ids"
