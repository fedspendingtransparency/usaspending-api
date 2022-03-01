import logging

from django.db import connections, transaction, DEFAULT_DB_ALIAS

from usaspending_api.awards.models import TransactionNormalized


logger = logging.getLogger("script")


@transaction.atomic
def delete_stale_fabs(ids_to_delete):
    """ids_to_delete are published_award_financial_assistance_ids"""
    logger.info("Starting deletion of stale FABS data")

    if not ids_to_delete:
        return []

    transactions = TransactionNormalized.objects.filter(
        assistance_data__published_award_financial_assistance_id__in=ids_to_delete
    )
    update_and_delete_award_ids = list(set(transactions.values_list("award_id", flat=True)))
    delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list("id")]
    delete_transaction_str_ids = ",".join([str(deleted_result) for deleted_result in delete_transaction_ids])

    if delete_transaction_ids:
        awards = (
            "update awards set latest_transaction_id = null, earliest_transaction_id = null "
            "where latest_transaction_id in ({ids}) or earliest_transaction_id in ({ids}) "
            "returning id".format(ids=delete_transaction_str_ids)
        )
        fabs = 'DELETE FROM "transaction_fabs" tf WHERE tf."transaction_id" IN ({});'
        tn = 'DELETE FROM "transaction_normalized" tn WHERE tn."id" IN ({});'
        ts = 'DELETE FROM "transaction_search" ts WHERE ts."transaction_id" IN ({});'
        td = "DELETE FROM transaction_delta td WHERE td.transaction_id in ({});"
        queries = [
            awards,
            fabs.format(delete_transaction_str_ids),
            tn.format(delete_transaction_str_ids),
            ts.format(delete_transaction_str_ids),
            td.format(delete_transaction_str_ids),
        ]

        db_query = "".join(queries)
        db_cursor = connections[DEFAULT_DB_ALIAS].cursor()
        db_cursor.execute(db_query, [])

    return update_and_delete_award_ids
