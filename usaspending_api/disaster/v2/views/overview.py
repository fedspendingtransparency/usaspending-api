from decimal import Decimal
from typing import Optional

from django.db.models import Sum, F
from rest_framework.response import Response

from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    filter_by_defc_closed_periods,
    latest_faba_of_each_year_queryset,
    latest_gtas_of_each_year_queryset,
)
from usaspending_api.references.models import DisasterEmergencyFundCode, GTASSF133Balances


class OverviewViewSet(DisasterBase):
    """
    This route gathers aggregate data about Disaster spending
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/overview.md"

    @cache_response()
    def get(self, request):

        request_values = self._parse_and_validate(request.GET)
        self.defc = request_values["def_codes"].split(",")
        funding, self.total_budget_authority = self.funding()

        return Response(
            {
                "funding": funding,
                "total_budget_authority": self.total_budget_authority,
                "spending": self.spending(),
                "additional": self.additional_totals() if self.defc == ["V"] else None,
            }
        )

    def _parse_and_validate(self, request):
        all_def_codes = sorted(list(DisasterEmergencyFundCode.objects.values_list("code", flat=True)))
        models = [
            {
                "key": "def_codes",
                "name": "def_codes",
                "type": "text",
                "text_type": "search",
                "allow_nulls": True,
                "optional": True,
                "default": ",".join(all_def_codes),
            },
        ]
        return TinyShield(models).block(request)

    def funding(self):
        funding = list(
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund__in=self.defc)
            .values("disaster_emergency_fund")
            .annotate(
                def_code=F("disaster_emergency_fund"),
                amount=(
                    Sum("total_budgetary_resources_cpe")
                    - (
                        Sum("budget_authority_unobligated_balance_brought_forward_cpe")
                        + Sum("adjustments_to_unobligated_balance_brought_forward_fyb")
                        + Sum("deobligations_or_recoveries_or_refunds_from_prior_year_cpe")
                        + Sum("prior_year_paid_obligation_recoveries")
                    )
                ),
            )
            .values("def_code", "amount")
        )

        total_budget_authority = self.sum_values(funding, "amount")

        return funding, total_budget_authority

    def spending(self):
        return {"award_obligations": self.award_obligations(), "award_outlays": self.award_outlays(), **self.totals()}

    def award_obligations(self):
        return (
            FinancialAccountsByAwards.objects.filter(
                filter_by_defc_closed_periods(), disaster_emergency_fund__in=self.defc
            )
            .values("transaction_obligated_amount")
            .aggregate(total=Sum("transaction_obligated_amount"))["total"]
            or 0.0
        )

    def award_outlays(self):
        return (
            latest_faba_of_each_year_queryset()
            .filter(disaster_emergency_fund__in=self.defc)
            .aggregate(
                total=(
                    Sum("gross_outlay_amount_by_award_cpe")
                    + Sum("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe")
                    + Sum("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe")
                )
            )["total"]
        ) or 0.0

    def totals(self) -> dict:
        obligation_values = [
            "obligations_incurred_total_cpe",
            "deobligations_or_recoveries_or_refunds_from_prior_year_cpe",
        ]
        outlay_values = ["gross_outlay_amount_by_tas_cpe", "anticipated_prior_year_obligation_recoveries"]

        results = (
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund__in=self.defc)
            .values(*obligation_values, *outlay_values)
            .aggregate(
                obligation_totals=(
                    Sum("obligations_incurred_total_cpe")
                    - (
                        Sum("deobligations_or_recoveries_or_refunds_from_prior_year_cpe")
                        + Sum("adjustments_to_unobligated_balance_brought_forward_fyb")
                    )
                ),
                outlay_totals=(
                    Sum("gross_outlay_amount_by_tas_cpe") - Sum("anticipated_prior_year_obligation_recoveries")
                ),
            )
        )

        return {
            "total_obligations": results["obligation_totals"] or 0.0,
            "total_outlays": results["outlay_totals"] or 0.0,
        }

    @staticmethod
    def sum_values(list_of_objects: list, key_to_extract: str) -> Decimal:
        return Decimal(sum([elem[key_to_extract] for elem in list_of_objects]))

    @staticmethod
    def additional_totals() -> Optional[dict]:
        """
        In the case where the filter is for only DEFC "V" this is a special case where some
        additional values are returned in the response to account for DEFC "V" values
        that were reported as DEFC "O".
        """
        obligation_values = [
            "obligations_incurred_total_cpe",
            "deobligations_or_recoveries_or_refunds_from_prior_year_cpe",
            "adjustments_to_unobligated_balance_brought_forward_fyb",
        ]
        outlay_values = ["gross_outlay_amount_by_tas_cpe", "anticipated_prior_year_obligation_recoveries"]

        filters = {
            "tas_rendering_label__in": ["016-X-0168-000", "016-X-1801-000", "016-X-8042-000"],
            "disaster_emergency_fund": "O",
            "treasury_account_identifier__funding_toptier_agency__abbreviation": "DOL",
        }

        aggregates = {
            "obligation_totals": (
                Sum("obligations_incurred_total_cpe")
                - Sum("deobligations_or_recoveries_or_refunds_from_prior_year_cpe")
                - Sum("adjustments_to_unobligated_balance_brought_forward_fyb")
            ),
            "outlay_totals": (
                Sum("gross_outlay_amount_by_tas_cpe") - Sum("anticipated_prior_year_obligation_recoveries")
            ),
        }
        defc_o_values = (
            GTASSF133Balances.objects.filter(**filters, fiscal_year=2021, fiscal_period=6)
            .values(*obligation_values, *outlay_values)
            .aggregate(**aggregates)
        )

        total_values = (
            latest_gtas_of_each_year_queryset().filter(**filters, fiscal_year__gte=2021).aggregate(**aggregates)
        )

        # This case should never occur outside of local and test environments;
        # in place to make sure we have values to avoid errors with None values
        if not all([val is not None for val in list(total_values.values()) + list(defc_o_values.values())]):
            return None

        # Obligation values are used to calculate the adjustment for TBR here to match the calculations
        # in DataLab. This should be capped at a specific period once new appropriates from DOL are not
        # associated with ARP.
        return {
            "spending": {
                "total_obligations": total_values["obligation_totals"] - defc_o_values["obligation_totals"],
                "total_outlays": total_values["outlay_totals"] - defc_o_values["outlay_totals"],
            },
            "total_budget_authority": total_values["obligation_totals"] - defc_o_values["obligation_totals"],
        }
