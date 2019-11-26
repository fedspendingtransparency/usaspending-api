from django.core.management.base import BaseCommand

from usaspending_api.transactions.agnostic_transaction_loader import AgnosticTransactionLoader
from usaspending_api.transactions.models.source_assistance_transaction import SourceAssistanceTransaction


class Command(AgnosticTransactionLoader, BaseCommand):
    help = "Upsert assistance transactions from a Broker database into an USAspending database"
    broker_source_table_name = SourceAssistanceTransaction().broker_source_table
    destination_table_name = SourceAssistanceTransaction().table_name
    last_load_record = "source_assistance_transaction"
    lookback_minutes = 15
    shared_pk = "published_award_financial_assistance_id"
    working_file_prefix = "assistance_load_ids"
    broker_select_sql = """
        with submissions as (
                select  submission_id
                from    submission
                where   d2_submission is true and
                        publish_status_id in (2, 3)
                        {optional_predicate}
        )
        SELECT {id}
        FROM {table}
        WHERE is_active IS true AND submission_id IN (SELECT * FROM submissions)
    """
