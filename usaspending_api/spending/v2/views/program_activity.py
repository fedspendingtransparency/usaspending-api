from django.db.models import F, Sum, CharField, Value


def program_activity(queryset, fiscal_year):
    # Program Activity Queryset
    pa = queryset.annotate(
        id=F('treasury_account__federal_account__main_account_code'),
        type=Value('program_activity', output_field=CharField()),
        name=F('program_activity__program_activity_name'),
        code=F('program_activity__program_activity_code'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    program_activity_total = pa.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in program_activity_total.items():
        program_activity_total = value

    program_activity_results = {
        'total': program_activity_total,
        'end_date': fiscal_year,
        'results': pa,
    }
    return program_activity_results
