from django.db.models import Q
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase


class CountBase(DisasterBase):
    def is_after_min_date(self):
        return Q(submission__reporting_period_start__gte=self.reporting_period_min)

    def is_in_provided_def_codes(self):
        return Q(disaster_emergency_fund__code__in=self.def_codes)

    def is_non_zero_object_class_cpe(self):
        return Q(
            Q(obligations_incurred_by_program_object_class_cpe__gt=0)
            | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
            | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
            | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
        )

    def is_non_zero_award_cpe(self):
        return Q(
            Q(obligations_incurred_total_by_award_cpe__gt=0)
            | Q(obligations_incurred_total_by_award_cpe__lt=0)
            | Q(gross_outlays_delivered_orders_paid_total_cpe__gt=0)
            | Q(gross_outlays_delivered_orders_paid_total_cpe__lt=0)
        )

    def is_provided_award_type(self):
        return Q(type__in=self.award_type_codes)
