import enum

from functools import lru_cache
from typing import Dict, Union

from django.db.models import DecimalField, F, Q, When, Case, Value
from django.db.models.functions import Coalesce

from usaspending_api.common.helpers.orm_helpers import sum_column_list


class CalculationType(enum.Enum):
    OBLIGATION = "obligation"
    OUTLAY = "outlay"


@lru_cache(maxsize=1)
def _obligation_columns() -> Dict[str, Union[str, F]]:
    """
    Returns the columns used to calculate the File B obligations broken down by PYA.
    """
    pya_columns = {
        "base_pya_x": "obligations_incurred_by_program_object_class_cpe",  # separated for non-zero calculations
        "pya_b": [
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
        "pya_p": [
            Coalesce("ussgl480110_rein_undel_ord_cpe", 0, output_field=DecimalField(max_digits=23, decimal_places=2)),
            Coalesce("ussgl490110_rein_deliv_ord_cpe", 0, output_field=DecimalField(max_digits=23, decimal_places=2)),
        ],
        "pya_x": ["deobligations_recoveries_refund_pri_program_object_class_cpe"],
    }

    # Some PYA values share columns, so we handle that here
    # !!! The order of the statements is important !!!
    pya_columns["pya_x"].extend(pya_columns["pya_p"])
    pya_columns["pya_p"].extend(pya_columns["pya_b"])

    # Loop through the lists of column names to create expressions we can use in QuerySets
    for pya_label, val in pya_columns.items():
        if isinstance(val, list):
            pya_columns[pya_label] = sum_column_list(val)

    return pya_columns


@lru_cache(maxsize=1)
def _outlay_columns() -> Dict[str, Union[str, F]]:
    """
    Returns the columns used to calculate the File B outlays broken down by PYA.
    """
    pya_columns = {
        "base_pya_x": "gross_outlay_amount_by_program_object_class_cpe",  # separated for non-zero calculations
        "pya_x": [
            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe",
            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
        ],
    }

    # Loop through the lists of column names to create expressions we can use in QuerySets
    for pya_label, val in pya_columns.items():
        if isinstance(val, list):
            pya_columns[pya_label] = sum_column_list(val)

    return pya_columns


def _calculate_obligations_and_outlays(
    calculation_type: str,
    *,
    is_multi_year: bool = False,
    include_final_sub_filter: bool = False,
) -> Case:
    """
    Builds the Case statement used to decide the Obligation or Outlay calculation that should be used.

    Args:
        calculation_type: Whether to calculate obligations or outlays

        is_multi_year: Determines what records should be included in a calculation based on PYA

        include_final_sub_filter: There are slightly different implementations for calculating the obligations and
            outlays for File B. Including this filter as optional allows for outside logic to select submissions.

    Returns:
        A single Case object is returned that adds together corresponding columns depending on their PYA value
        and optional filtering
    """
    calculation_type = CalculationType(calculation_type)

    if is_multi_year and calculation_type == CalculationType.OUTLAY:
        raise ValueError("'is_multi_year' parameter is not currently supported for calculation_type of 'outlay'")

    columns = _obligation_columns() if calculation_type == CalculationType.OBLIGATION else _outlay_columns()
    final_sub_filter = Q(submission__is_final_balances_for_fy=True) if include_final_sub_filter else Q()

    when_statements = [
        When(
            final_sub_filter & Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True)),
            then=F(columns["base_pya_x"]) + columns["pya_x"],
        )
    ]

    if is_multi_year:
        when_statements.extend(
            [
                When(
                    final_sub_filter & Q(prior_year_adjustment="B"),
                    then=columns["pya_b"],
                ),
                When(
                    final_sub_filter & Q(prior_year_adjustment="P"),
                    then=columns["pya_p"],
                ),
            ]
        )

    return Case(
        *when_statements,
        default=Value(0),
        output_field=DecimalField(max_digits=23, decimal_places=2),
    )


def is_non_zero_total_spending() -> Q:
    """
    In the case of single year calculation there is a possibility that a File B record contains
    obligations and outlays that amount to a total of zero. In some contexts we would want to filter
    those values out to keep our calculations from displaying them towards totals.
    """
    obligation_columns = _obligation_columns()
    outlay_columns = _outlay_columns()
    return ~Q(
        Q(
            Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True))
            & Q(**{obligation_columns["base_pya_x"]: obligation_columns["pya_x"] * -1})
        )
        & Q(
            Q(Q(prior_year_adjustment="X") | Q(prior_year_adjustment__isnull=True))
            & Q(**{outlay_columns["base_pya_x"]: outlay_columns["pya_x"] * -1})
        )
    )


def get_obligations(*, is_multi_year: bool = False, include_final_sub_filter: bool = False) -> Case:
    """
    Builds the Case statement used to decide the Obligation calculation that should be used
    """
    return _calculate_obligations_and_outlays(
        "obligation", is_multi_year=is_multi_year, include_final_sub_filter=include_final_sub_filter
    )


def get_outlays(*, include_final_sub_filter: bool = False) -> Case:
    """
    Builds the Case statement used to decide the Outlay calculation that should be used
    """
    return _calculate_obligations_and_outlays("outlay", include_final_sub_filter=include_final_sub_filter)
