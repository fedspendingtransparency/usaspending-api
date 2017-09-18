from django.db.models import F, Sum, Value, CharField, Q
from decimal import Decimal
from usaspending_api.references.models import Agency


class Explorer(object):
    def __init__(self, alt_set, queryset):
        self.alt_set = alt_set
        self.queryset = queryset

    def budget_function(self):
        # Budget Function Queryset
        queryset = self.queryset.annotate(
            id=F('treasury_account__budget_function_code'),
            type=Value('budget_function', output_field=CharField()),
            name=F('treasury_account__budget_function_title'),
            code=F('treasury_account__budget_function_code'),
        ).values('id', 'type', 'name', 'code', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        return queryset

    def budget_subfunction(self):
        # Budget Sub Function Queryset
        queryset = self.queryset.annotate(
            id=F('treasury_account__budget_subfunction_code'),
            type=Value('budget_subfunction', output_field=CharField()),
            name=F('treasury_account__budget_subfunction_title'),
            code=F('treasury_account__budget_subfunction_code')
        ).values('id', 'type', 'name', 'code', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        return queryset

    def federal_account(self):
        # Federal Account Queryset
        queryset = self.queryset.annotate(
            id=F('treasury_account__federal_account'),
            type=Value('federal_account', output_field=CharField()),
            name=F('treasury_account__federal_account__account_title'),
            code=F('treasury_account__federal_account__main_account_code')
        ).values(
            'id', 'type', 'name', 'code', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        return queryset

    def program_activity(self):
        # Program Activity Queryset
        queryset = self.queryset.annotate(
            id=F('program_activity'),
            type=Value('program_activity', output_field=CharField()),
            name=F('program_activity__program_activity_name'),
            code=F('program_activity__program_activity_code')
        ).values(
            'id', 'type', 'name', 'code', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        return queryset

    def object_class(self):
        # Object Classes Queryset
        queryset = self.queryset.annotate(
            id=F('object_class__major_object_class'),
            type=Value('object_class', output_field=CharField()),
            name=F('object_class__major_object_class_name'),
            code=F('object_class__major_object_class')
        ).values(
            'id', 'type', 'name', 'code', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        return queryset

    def recipient(self):
        # Recipients Queryset
        alt_set = self.alt_set.filter(~Q(transaction_obligated_amount=Decimal('NaN')), award__recipient__isnull=False). \
            annotate(id=F('award__recipient__legal_entity_id'), type=Value('recipient', output_field=CharField()),
                     name=F('award__recipient__recipient_name'), code=F('award__recipient__recipient_unique_id')).\
            values('id', 'type', 'name', 'code', 'amount').\
            annotate(total=Sum('transaction_obligated_amount')).\
            order_by('-total')

        return alt_set

    def agency(self):
        # Funding Top Tier Agencies Querysets

        agency_queryet = Agency.objects.filter(toptier_flag=True).values('id', 'toptier_agency_id')
        agency_ids = {agency['toptier_agency_id']: agency['id'] for agency in agency_queryet}

        queryset = self.queryset.filter(treasury_account__funding_toptier_agency__isnull=False).annotate(
            id=F('treasury_account__funding_toptier_agency_id'),
            type=Value('agency', output_field=CharField()),
            name=F('treasury_account__funding_toptier_agency__name'),
            code=F('treasury_account__funding_toptier_agency__cgac_code')
        ).values('id', 'type', 'code', 'name', 'amount').annotate(
            total=Sum('obligations_incurred_by_program_object_class_cpe')).order_by('-total')

        for element in queryset:
            element['id'] = agency_ids[element['id']]

        return queryset

    def award_category(self):
        # Award Category Queryset
        alt_set = self.alt_set.annotate(
            id=F('award__id'),
            type=Value('award_category', output_field=CharField()),
            name=F('award__category')
        ).values('id', 'type', 'piid', 'fain', 'uri', 'name', 'amount').annotate(
            total=Sum('transaction_obligated_amount')).order_by('-total')

        return alt_set

    def award(self):
        # Awards Queryset
        alt_set = self.alt_set.annotate(
            id=F('award__id'),
            type=Value('award', output_field=CharField())
        ).values('id', 'type', 'piid', 'fain', 'uri', 'amount').annotate(
            total=Sum('transaction_obligated_amount')).order_by('-total')

        return alt_set
