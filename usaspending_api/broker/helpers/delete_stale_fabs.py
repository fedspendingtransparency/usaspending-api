import logging

from django.db import connections, transaction

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.broker.helpers.find_related_awards import find_related_awards


logger = logging.getLogger("console")


@transaction.atomic
def delete_stale_fabs(ids_to_delete):
    logger.info('Starting deletion of stale FABS data')

    if not ids_to_delete:
        return []

    transactions = TransactionNormalized.objects.filter(assistance_data__afa_generated_unique__in=ids_to_delete)
    update_award_ids, delete_award_ids = find_related_awards(transactions)

    delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list('id')]
    delete_transaction_str_ids = ','.join([str(deleted_result) for deleted_result in delete_transaction_ids])
    update_award_str_ids = ','.join([str(update_result) for update_result in update_award_ids])
    delete_award_str_ids = ','.join([str(deleted_result) for deleted_result in delete_award_ids])

    db_cursor = connections['default'].cursor()

    queries = []
    # Transaction FABS
    if delete_transaction_ids:
        fabs = 'DELETE FROM "transaction_fabs" tf WHERE tf."transaction_id" IN ({});'
        tn = 'DELETE FROM "transaction_normalized" tn WHERE tn."id" IN ({});'
        queries.extend([fabs.format(delete_transaction_str_ids), tn.format(delete_transaction_str_ids)])
    # Update Awards
    if update_award_ids:
        # Adding to award_update_id_list so the latest_transaction will be recalculated
        update_awards = 'UPDATE "awards" SET "latest_transaction_id" = null WHERE "id" IN ({});'
        update_awards_query = update_awards.format(update_award_str_ids)
        queries.append(update_awards_query)
    if delete_award_ids:
        # Financial Accounts by Awards
        faba = 'UPDATE "financial_accounts_by_awards" SET "award_id" = null WHERE "award_id" IN ({});'
        # Subawards
        sub = 'UPDATE "subaward" SET "award_id" = null WHERE "award_id" IN ({});'.format(delete_award_str_ids)
        # Delete Awards
        delete_awards_query = 'DELETE FROM "awards" a WHERE a."id" IN ({});'.format(delete_award_str_ids)
        queries.extend([faba.format(delete_award_str_ids), sub, delete_awards_query])
    if queries:
        db_query = ''.join(queries)
        db_cursor.execute(db_query, [])
    return update_award_ids
