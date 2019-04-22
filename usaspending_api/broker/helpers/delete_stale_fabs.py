import logging

from django.db import connections, transaction

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.broker.helpers.find_related_awards import find_related_awards
from usaspending_api.broker.helpers.find_related_legal_entities import find_related_legal_entities
from usaspending_api.broker.helpers.find_related_locations import find_related_locations


logger = logging.getLogger("console")


@transaction.atomic
def delete_stale_fabs(ids_to_delete):
    logger.info('Starting deletion of stale FABS data')

    if not ids_to_delete:
        return []

    transactions = TransactionNormalized.objects.filter(assistance_data__afa_generated_unique__in=ids_to_delete)
    update_award_ids, delete_award_ids = find_related_awards(transactions)
    delete_le_ids = find_related_legal_entities(transactions)
    delete_loc_ids = find_related_locations(transactions)

    delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list('id')]
    delete_transaction_str_ids = ','.join([str(deleted_result) for deleted_result in delete_transaction_ids])
    delete_award_str_ids = ','.join([str(deleted_result) for deleted_result in delete_award_ids])
    delete_le_str_ids = ','.join([str(deleted_result) for deleted_result in delete_le_ids])
    delete_loc_str_ids = ','.join([str(deleted_result) for deleted_result in delete_loc_ids])

    queries = []
    # Transaction FABS
    if delete_transaction_ids:
        fabs = 'DELETE FROM "transaction_fabs" tf WHERE tf."transaction_id" IN ({});'
        tn = 'DELETE FROM "transaction_normalized" tn WHERE tn."id" IN ({});'
        queries.extend([fabs.format(delete_transaction_str_ids), tn.format(delete_transaction_str_ids)])
    if delete_award_ids:
        # Financial Accounts by Awards
        faba = 'UPDATE "financial_accounts_by_awards" SET "award_id" = null WHERE "award_id" IN ({});'
        # Subawards
        sub = 'UPDATE "subaward" SET "award_id" = null WHERE "award_id" IN ({});'.format(delete_award_str_ids)
        # Delete Awards
        delete_awards_query = 'DELETE FROM "awards" a WHERE a."id" IN ({});'.format(delete_award_str_ids)
        queries.extend([faba.format(delete_award_str_ids), sub, delete_awards_query])
    if delete_le_str_ids:
        # Delete unused legal entities
        delete_awards_query = 'DELETE FROM "legal_entity" le WHERE le."legal_entity_id" IN ({});'
        queries.extend([delete_awards_query.format(delete_le_str_ids)])
    if delete_loc_str_ids:
        # Delete unused locations
        delete_awards_query = 'DELETE FROM "references_location" loc WHERE loc."location_id" IN ({});'
        queries.extend([delete_awards_query.format(delete_loc_str_ids)])

    if queries:
        db_query = ''.join(queries)
        db_cursor = connections['default'].cursor()
        db_cursor.execute(db_query, [])

    # Update Awards
    update_awards(tuple(update_award_ids))

    return update_award_ids
