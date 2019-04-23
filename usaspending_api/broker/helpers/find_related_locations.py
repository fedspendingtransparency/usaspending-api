from django.db.models import Count
from usaspending_api.awards.models import TransactionNormalized


def find_related_locations(transactions):
    related_ppop_ids = transactions.values_list('place_of_performance_id', flat=True)
    ppop_tn_count = (
        TransactionNormalized.objects.filter(place_of_performance_id__in=related_ppop_ids)
        .values('place_of_performance_id')
        .annotate(transaction_count=Count('id'))
        .values_list('place_of_performance_id', 'transaction_count')
    )
    ppop_tn_count_filtered = (
        transactions.values('place_of_performance_id')
        .annotate(transaction_count=Count('id'))
        .values_list('place_of_performance_id', 'transaction_count')
    )
    ppop_tn_count_mapping = dict(ppop_tn_count)
    ppop_tn_count_filtered_mapping = dict(ppop_tn_count_filtered)
    # only ppop locations if and only if all their transactions are deleted
    delete_ppop_ids = list(dict(ppop_tn_count_mapping .items() & ppop_tn_count_filtered_mapping.items()))

    related_le_loc_ids = transactions.values_list('recipient__location_id', flat=True)
    le_tn_count = (
        TransactionNormalized.objects.filter(recipient__location_id__in=related_le_loc_ids)
        .values('recipient__location_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient__location_id', 'transaction_count')
    )
    le_tn_count_filtered = (
        transactions.values('recipient__location_id')
        .annotate(transaction_count=Count('id'))
        .values_list('recipient__location_id', 'transaction_count')
    )
    le_tn_count_mapping = dict(le_tn_count)
    le_tn_count_filtered_mapping = dict(le_tn_count_filtered)
    # only delete legal entity locations if and only if all their transactions are deleted
    delete_le_loc_ids = list(dict(le_tn_count_mapping.items() & le_tn_count_filtered_mapping.items()))

    delete_loc_ids = list(set(delete_ppop_ids + delete_le_loc_ids))
    return delete_loc_ids
