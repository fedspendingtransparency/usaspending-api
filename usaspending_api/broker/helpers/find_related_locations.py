from django.db.models import Count
from usaspending_api.awards.models import TransactionNormalized


def find_related_locations(transactions):
    related_ppop_ids = [result[0] for result in transactions.values_list('place_of_performance_id')]
    tn_count = (
        TransactionNormalized.objects.filter(place_of_performance_id__in=related_ppop_ids)
        .values('place_of_performance_id')
        .annotate(transaction_count=Count('id'))
        .values_list('place_of_performance_id', 'transaction_count')
    )
    tn_count_filtered = (
        transactions.values('place_of_performance_id')
        .annotate(transaction_count=Count('id'))
        .values_list('place_of_performance_id', 'transaction_count')
    )
    tn_count_mapping = {ppop_id: transaction_count for ppop_id, transaction_count in tn_count}
    tn_count_filtered_mapping = {ppop_id: transaction_count for ppop_id, transaction_count in tn_count_filtered}
    # only delete legal entities if and only if all their transactions are deleted
    delete_ppop_ids = [
        ppop_id
        for ppop_id, transaction_count in tn_count_mapping.items()
        if tn_count_filtered_mapping[ppop_id] == transaction_count
    ]

    related_le_loc_ids = [result[0] for result in transactions.values_list('recipient__location_id')]
    tn_count = (
        TransactionNormalized.objects.filter(recipient__location_id__in=related_le_loc_ids)
        .values('recipient__location_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient__location_id', 'transaction_count')
    )
    tn_count_filtered = (
        transactions.values('recipient__location_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient__location_id', 'transaction_count')
    )
    tn_count_mapping = {le_loc_id: transaction_count for le_loc_id, transaction_count in tn_count}
    tn_count_filtered_mapping = {le_loc_id: transaction_count for le_loc_id, transaction_count in tn_count_filtered}
    # only delete legal entities if and only if all their transactions are deleted
    delete_le_loc_ids = [
        le_loc_id
        for le_loc_id, transaction_count in tn_count_mapping.items()
        if tn_count_filtered_mapping[le_loc_id] == transaction_count
    ]

    delete_loc_ids = list(set(delete_ppop_ids + delete_le_loc_ids))
    return delete_loc_ids
