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

    def is_last_closed_submission_window(self):
        sub_queries = []
        if self.last_closed_monthly_submission_dates:
            sub_queries.append(
                Q(
                    submission__quarter_format_flag=False,
                    submission__reporting_fiscal_year__lte=self.last_closed_monthly_submission_dates.get(
                        "submission_fiscal_year"
                    ),
                    submission__reporting_fiscal_period__lte=self.last_closed_monthly_submission_dates.get(
                        "submission_fiscal_month"
                    ),
                )
            )

        sub_queries.append(
            Q(
                submission__quarter_format_flag=True,
                submission__reporting_fiscal_year__lte=self.last_closed_quarterly_submission_dates.get(
                    "submission_fiscal_year"
                ),
                submission__reporting_fiscal_quarter__lte=self.last_closed_quarterly_submission_dates.get(
                    "submission_fiscal_quarter"
                ),
            )
        )
        sub_queryset = sub_queries.pop()
        for query in sub_queries:
            sub_queryset |= query
        return sub_queryset
