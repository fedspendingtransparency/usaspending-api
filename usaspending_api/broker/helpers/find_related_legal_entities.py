from django.db.models import Count
from usaspending_api.awards.models import TransactionNormalized


def find_related_legal_entities(transactions):
    related_le_ids = [result[0] for result in transactions.values_list('recipient_id')]
    tn_count = (
        TransactionNormalized.objects.filter(recipient_id__in=related_le_ids)
        .values('recipient_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient_id', 'transaction_count')
    )
    tn_count_filtered = (
        transactions.values('recipient_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient_id', 'transaction_count')
    )
    tn_count_mapping = {le_id: transaction_count for le_id, transaction_count in tn_count}
    tn_count_filtered_mapping = {le_id: transaction_count for le_id, transaction_count in tn_count_filtered}
    # only delete legal entities if and only if all their transactions are deleted
    delete_les = [
        le_id
        for le_id, transaction_count in tn_count_mapping.items()
        if tn_count_filtered_mapping[le_id] == transaction_count
    ]
    return delete_les
