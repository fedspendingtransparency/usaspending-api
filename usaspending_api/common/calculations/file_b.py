import enum
from functools import cached_property
from typing import Dict, Union

from django.db.models import Case, DecimalField, F, Q, Value, When
from django.db.models.functions import Coalesce

from usaspending_api.common.helpers.orm_helpers import sum_column_list


class CalculationType(enum.Enum):
    OBLIGATION = "obligation"
    OUTLAY = "outlay"


class FileBCalculations:

    def __init__(self, *, include_final_sub_filter: bool = False, is_covid_page: bool = False) -> None:
        """
        Args:
            include_final_sub_filter: In some cases the File B calculations are filtered to a specific Fiscal Year
                and Quarter / Period or specific Submissions elsewhere in the queryset. However, it is a common case
                that the Obligation and Outlay calculations should filter to only those Submissions that belong to
                the latest Submission period that has been revealed.

            is_covid_page: While it is possible that this logic could be shared by other endpoints in the future,
                the COVID Page is the only page that supports this "multi year" functionality because it has a
                defined start date (2020-04-01) and we support corrections to that data via USSGL and PYA.
                This has been named accordingly to prevent possible confusion that this could be used for any
                multi year calculations, but instead should be used sparingly; rename with care.
        """
        self.include_final_sub_filter = include_final_sub_filter
        self.is_covid_page = is_covid_page

    def _build_columns(
        self, base_column: str, pya_columns: Dict[str, Union[str, Coalesce]]
    ) -> Dict[str, Union[str, F]]:
        """
        Loop through the lists of column names to create expressions we can use in QuerySets
        """
        for pya_label, val in pya_columns.items():
            pya_columns[pya_label] = sum_column_list(val)

        return {"base": base_column, **pya_columns}

    @cached_property
    def obligation_columns(self) -> Dict[str, Union[str, F]]:
        """
        Creates a dictionary of the different PYA columns used when calculating obligations.
        """
        pya_columns = {
            "B": [
                "ussgl480100_undelivered_orders_obligations_unpaid_cpe",
                "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe",
                "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe",
                "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe",
                "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe",
                "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe",
                "ussgl490100_delivered_orders_obligations_unpaid_cpe",
                "ussgl490200_delivered_orders_obligations_paid_cpe",
                "ussgl490800_authority_outlayed_not_yet_disbursed_cpe",
                "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe",
                "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
                "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe",
                "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe",
            ],
            "P": [
                Coalesce(
                    "ussgl480110_rein_undel_ord_cpe", 0, output_field=DecimalField(max_digits=23, decimal_places=2)
                ),
                Coalesce(
                    "ussgl490110_rein_deliv_ord_cpe", 0, output_field=DecimalField(max_digits=23, decimal_places=2)
                ),
            ],
            "X": ["deobligations_recoveries_refund_pri_program_object_class_cpe"],
        }

        # Some PYA values share columns, so we handle that here
        # !!! The order of the statements is important !!!
        pya_columns["P"].extend(pya_columns["B"])

        return self._build_columns("obligations_incurred_by_program_object_class_cpe", pya_columns)

    @cached_property
    def outlay_columns(self) -> Dict[str, Union[str, F]]:
        """
        Creates a dictionary of the different PYA columns used when calculating outlays.
        """
        pya_columns = {
            "X": [
                "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe",
                "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
            ],
        }
        return self._build_columns("gross_outlay_amount_by_program_object_class_cpe", pya_columns)

    def _calculate(self, calculation_type: CalculationType) -> Case:
        """
        Builds the Case statement used to decide the Obligation or Outlay calculation that should be used.
        """
        columns = self.obligation_columns if calculation_type == CalculationType.OBLIGATION else self.outlay_columns
        final_sub_filter = Q(submission__is_final_balances_for_fy=True) if self.include_final_sub_filter else Q()

        if self.is_covid_page:
            when_statements = [
                When(
                    final_sub_filter & Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True)),
                    then=F(columns["base"]) + columns["X"],
                )
            ]
            when_statements.extend(
                [
                    When(
                        final_sub_filter & Q(prior_year_adjustment=pya_value),
                        then=columns[pya_value],
                    )
                    for pya_value in set(columns) - {"base", "X"}
                ]
            )
        else:
            when_statements = [
                When(
                    final_sub_filter & Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True)),
                    then=F(columns["base"]),
                )
            ]

        return Case(
            *when_statements,
            default=Value(0),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        )

    def get_obligations(self) -> Case:
        return self._calculate(CalculationType.OBLIGATION)

    def get_outlays(self) -> Case:
        return self._calculate(CalculationType.OUTLAY)

    def is_non_zero_total_spending(self) -> Q:
        """
        Filter that removes records where the obligations and outlays are either equal to zero
        or are adjusted to negate the original obligations and outlays.
        """
        if self.is_covid_page:
            result = Q(
                # should automatically include records with a PYA != X or NULL
                Q(prior_year_adjustment="B")
                | Q(prior_year_adjustment="P")
                | Q(
                    # in the case of PYA X or NULL we validate that the obligations and outlays are nonzero
                    Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True))
                    # filter out records where obligations or outlays are negated
                    & ~Q(
                        Q(**{self.obligation_columns["base"]: self.obligation_columns["X"] * -1})
                        & Q(**{self.outlay_columns["base"]: self.outlay_columns["X"] * -1})
                    )
                )
            )
        else:
            result = Q(
                Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True))
                # filter out records that contain obligation or outlay amounts that equal zero
                & Q(
                    Q(**{f"{self.obligation_columns['base']}__lt": 0})
                    | Q(**{f"{self.obligation_columns['base']}__gt": 0})
                    | Q(**{f"{self.outlay_columns['base']}__lt": 0})
                    | Q(**{f"{self.outlay_columns['base']}__gt": 0})
                )
            )

        return result
