from django.db.models import Count
from usaspending_api.awards.models import TransactionNormalized


def find_related_awards(transactions):
    related_award_ids = transactions.values_list('award_id', flat=True)
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
    tn_count_mapping = dict(tn_count)
    tn_count_filtered_mapping = dict(tn_count_filtered)
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
