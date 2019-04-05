import boto3
import logging
import time

from datetime import datetime, timezone
from django.conf import settings
from django.db import connections, transaction
from django.db.models import Count

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.helpers.generic_helper import timer


logger = logging.getLogger("console")


def find_related_awards(transactions):
    related_award_ids = [result[0] for result in transactions.values_list('award_id')]
    tn_count = (
        TransactionNormalized.objects.filter(award_id__in=related_award_ids)
        .values('award_id')
        .annotate(transaction_count=Count('id'))
        .values_list('award_id', 'transaction_count')
    )
    tn_count_filtered = (
        transactions.values('award_id')
        .annotate(transaction_count=Count('id'))
        .values_list('award_id', 'transaction_count')
    )
    tn_count_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count}
    tn_count_filtered_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count_filtered}
    # only delete awards if and only if all their transactions are deleted, otherwise update the award
    update_awards = [
        award_id
        for award_id, transaction_count in tn_count_mapping.items()
        if tn_count_filtered_mapping[award_id] != transaction_count
    ]
    delete_awards = [
        award_id
        for award_id, transaction_count in tn_count_mapping.items()
        if tn_count_filtered_mapping[award_id] == transaction_count
    ]
    return update_awards, delete_awards


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


def store_deleted_fabs(ids_to_delete):
    seconds = int(time.time())  # adds enough uniqueness to filename
    file_name = datetime.now(timezone.utc).strftime('%Y-%m-%d') + "_FABSdeletions_" + str(seconds) + ".csv"
    file_with_headers = ['afa_generated_unique'] + list(ids_to_delete)

    if settings.IS_LOCAL:
        file_path = settings.CSV_LOCAL_PATH + file_name
        logger.info("storing deleted transaction IDs at: {}".format(file_path))
        with open(file_path, 'w') as writer:
            for row in file_with_headers:
                writer.write(row + '\n')
    else:
        logger.info('Uploading FABS delete data to S3 bucket')
        aws_region = settings.USASPENDING_AWS_REGION
        fpds_bucket_name = settings.FPDS_BUCKET_NAME
        s3client = boto3.client('s3', region_name=aws_region)
        contents = bytes()
        for row in file_with_headers:
            contents += bytes('{}\n'.format(row).encode())
        s3client.put_object(Bucket=fpds_bucket_name, Key=file_name, Body=contents)


def delete_fabs_transactions(ids_to_delete, do_not_log_deletions):
    """
    ids_to_delete are afa_generated_unique ids
    """
    if ids_to_delete:
        if do_not_log_deletions is False:
            store_deleted_fabs(ids_to_delete)
        with timer("deleting stale FABS data", logger.info):
            update_award_ids = delete_stale_fabs(ids_to_delete)

    else:
        update_award_ids = []
        logger.info("Nothing to delete...")

    return update_award_ids
