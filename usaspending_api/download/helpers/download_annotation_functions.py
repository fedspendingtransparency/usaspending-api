import datetime
from typing import List, Optional

from django.db.models.functions import Cast, Coalesce
from django.db.models import (
    Case,
    DateField,
    DecimalField,
    ExpressionWrapper,
    F,
    Func,
    OuterRef,
    Q,
    Subquery,
    Sum,
    TextField,
    Value,
    When,
)

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.models.transaction_normalized import NORM_TO_TRANSACTION_SEARCH_COL_MAP
from usaspending_api.common.helpers.orm_helpers import ConcatAll, FiscalYear, StringAggWithDefault, CFDAs
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import (
    filter_by_latest_closed_periods,
    final_submissions_for_all_fy,
)
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.settings import HOST

COVID_19_PERIOD_START = datetime.date(2020, 4, 1)
IIJA_PERIOD_START = datetime.date(2021, 11, 15)

AWARD_URL = f"{HOST}/award/" if "localhost" in HOST else f"https://{HOST}/award/"

CONGRESSIONAL_DISTRICT_DISPLAY_NAME_SEP = "-"


def congressional_district_display_name(state_column_name, cd_column_name):
    expression = Case(
        When(
            Q(
                Q(**{f"{state_column_name}__isnull": False})
                & Q(**{f"{cd_column_name}__isnull": False})
                & ~Q(**{f"{state_column_name}__exact": ""})
            ),
            then=ConcatAll(F(state_column_name), Value(CONGRESSIONAL_DISTRICT_DISPLAY_NAME_SEP), F(cd_column_name)),
        ),
        default=F(cd_column_name),
        output_field=TextField(),
    )
    return expression


AWARD_SEARCH_CD_DISPLAY_ANNOTATIONS = {
    # For state and congressional code we are not using the FPDS or FABS map since
    # both FPDS and FABS map to the same value.
    "prime_award_summary_recipient_cd_original": congressional_district_display_name(
        "latest_transaction_search__recipient_location_state_code",
        "latest_transaction_search__recipient_location_congressional_code",
    ),
    "prime_award_summary_recipient_cd_current": congressional_district_display_name(
        "latest_transaction_search__recipient_location_state_code",
        "latest_transaction_search__recipient_location_congressional_code_current",
    ),
    "prime_award_summary_place_of_performance_cd_original": congressional_district_display_name(
        "latest_transaction_search__pop_state_code",
        "latest_transaction_search__pop_congressional_code",
    ),
    "prime_award_summary_place_of_performance_cd_current": congressional_district_display_name(
        "latest_transaction_search__pop_state_code",
        "latest_transaction_search__pop_congressional_code_current",
    ),
}

TXN_SEARCH_CD_DISPLAY_ANNOTATIONS = {
    "prime_award_transaction_recipient_cd_original": congressional_district_display_name(
        "recipient_location_state_code",
        "recipient_location_congressional_code",
    ),
    "prime_award_transaction_recipient_cd_current": congressional_district_display_name(
        "recipient_location_state_code",
        "recipient_location_congressional_code_current",
    ),
    "prime_award_transaction_place_of_performance_cd_original": congressional_district_display_name(
        "pop_state_code",
        "pop_congressional_code",
    ),
    "prime_award_transaction_place_of_performance_cd_current": congressional_district_display_name(
        "pop_state_code",
        "pop_congressional_code_current",
    ),
}


def filter_limit_to_closed_periods(submission_query_path: str = "") -> Q:
    """Return Django Q for all closed submissions (quarterly and monthly)"""
    q = Q()
    for sub in final_submissions_for_all_fy():
        q |= (
            Q(**{f"{submission_query_path}submission__reporting_fiscal_year": sub.fiscal_year})
            & Q(**{f"{submission_query_path}submission__quarter_format_flag": sub.is_quarter})
            & Q(**{f"{submission_query_path}submission__reporting_fiscal_period__lte": sub.fiscal_period})
        )
    if not q:
        q = Q(pk__isnull=True)
    return q


def _disaster_emergency_fund_codes(
    def_codes: Optional[List[str]] = None, award_id_col: Optional[str] = "award_id"
) -> Subquery:
    filters = [filter_limit_to_closed_periods(), Q(award_id=OuterRef(award_id_col))]
    if def_codes:
        filters.append(Q(disaster_emergency_fund__code__in=def_codes))

    return Subquery(
        FinancialAccountsByAwards.objects.filter(*filters)
        .annotate(
            value=ExpressionWrapper(
                Case(
                    When(
                        disaster_emergency_fund__code__isnull=False,
                        then=ConcatAll(
                            F("disaster_emergency_fund__code"),
                            Value(": "),
                            F("disaster_emergency_fund__public_law"),
                        ),
                    ),
                    default=Value(None, output_field=TextField()),
                    output_field=TextField(),
                ),
                output_field=TextField(),
            )
        )
        .values("award_id")
        .annotate(total=StringAggWithDefault("value", ";", distinct=True))
        .values("total"),
        output_field=TextField(),
    )


def _obligation_txn_amount_agg_subquery(
    emergency_fund_group_name: str,
    def_codes: Optional[List[str]] = None,
    award_id_col: Optional[str] = "award_id",
) -> Subquery:
    filters = [
        filter_limit_to_closed_periods(),
        Q(award_id=OuterRef(award_id_col)),
        Q(disaster_emergency_fund__group_name=emergency_fund_group_name),
    ]
    if def_codes:
        filters.append(Q(disaster_emergency_fund__code__in=def_codes))

    return Subquery(
        FinancialAccountsByAwards.objects.filter(*filters)
        .values("award_id")
        .annotate(sum=Sum("transaction_obligated_amount"))
        .values("sum"),
        output_field=DecimalField(max_digits=23, decimal_places=2),
    )


def _outlay_amount_agg_subquery(
    emergency_fund_group_name: str,
    def_codes: Optional[List[str]] = None,
    award_id_col: Optional[str] = "award_id",
) -> Subquery:
    filters = [
        filter_by_latest_closed_periods(),
        Q(award_id=OuterRef(award_id_col)),
        Q(disaster_emergency_fund__group_name=emergency_fund_group_name),
    ]
    if def_codes:
        filters.append(Q(disaster_emergency_fund__code__in=def_codes))

    return Subquery(
        FinancialAccountsByAwards.objects.filter(*filters)
        .values("award_id")
        .annotate(
            sum=Coalesce(
                Sum("gross_outlay_amount_by_award_cpe"), 0, output_field=DecimalField(max_digits=23, decimal_places=2)
            )
            + Coalesce(
                Sum("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe"),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            )
            + Coalesce(
                Sum("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            )
        )
        .values("sum"),
        output_field=DecimalField(max_digits=23, decimal_places=2),
    )


def _outlay_amount_agg_subquery_no_coalesce(
    emergency_fund_group_name: str = None,
    def_codes: Optional[List[str]] = None,
    award_id_col: Optional[str] = "award_id",
) -> Subquery:
    """This function returns a total outlay aggregation
    where NULLs are not represented as 0s. This function
    is useful for when you want to show data that doesn't have outlay values
    as a blank/NULL (i.e. do NOT show “0”).
    """
    filters = [filter_by_latest_closed_periods(), Q(award_id=OuterRef(award_id_col))]
    if emergency_fund_group_name is not None:
        filters.append(Q(disaster_emergency_fund__group_name=emergency_fund_group_name))
    if def_codes:
        filters.append(Q(disaster_emergency_fund__code__in=def_codes))

    return Subquery(
        FinancialAccountsByAwards.objects.filter(*filters)
        .values("award_id")
        .annotate(
            total_gross_outlay_amount_by_award_cpe=Sum("gross_outlay_amount_by_award_cpe"),
            total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=Sum(
                "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe"
            ),
            total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=Sum(
                "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"
            ),
        )
        .filter(
            *[
                Q(
                    Q(total_gross_outlay_amount_by_award_cpe__isnull=False)
                    | Q(total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe__isnull=False)
                    | Q(total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe__isnull=False)
                )
            ]
        )
        .annotate(
            sum=Coalesce(
                "total_gross_outlay_amount_by_award_cpe", 0, output_field=DecimalField(max_digits=23, decimal_places=2)
            )
            + Coalesce(
                "total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe",
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            )
            + Coalesce(
                "total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            )
        )
        .values("sum"),
        output_field=DecimalField(max_digits=23, decimal_places=2),
    )


def transaction_search_annotations(filters: dict, file_type: str = None):
    """
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values
    """
    def_codes = filters.get("def_codes", [])
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(
                value=StringAggWithDefault(
                    "treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": ConcatAll(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                action_date__gte=datetime.date(2020, 4, 1),
                then=_disaster_emergency_fund_codes(def_codes=def_codes),
            ),
            output_field=TextField(),
        ),
        "outlayed_amount_from_COVID-19_supplementals_for_overall_award": _outlay_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            def_codes=def_codes,
        ),
        "obligated_amount_from_COVID-19_supplementals_for_overall_award": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            def_codes=def_codes,
        ),
        "outlayed_amount_from_IIJA_supplemental_for_overall_award": _outlay_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            def_codes=def_codes,
        ),
        "obligated_amount_from_IIJA_supplemental_for_overall_award": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            def_codes=def_codes,
        ),
        "total_outlayed_amount_for_overall_award": _outlay_amount_agg_subquery_no_coalesce(award_id_col="award_id"),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), object_class_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), program_activity_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
    }
    annotation_fields.update(TXN_SEARCH_CD_DISPLAY_ANNOTATIONS)
    return annotation_fields


def award_annotations(filters: dict, file_type: str = None):
    """
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values
    """
    def_codes = filters.get("def_codes", [])
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(
                value=StringAggWithDefault(
                    "treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": ConcatAll(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes"
        + NAMING_CONFLICT_DISCRIMINATOR: _disaster_emergency_fund_codes(def_codes=def_codes, award_id_col="award_id"),
        "outlayed_amount_from_COVID-19_supplementals": _outlay_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            def_codes=def_codes,
            award_id_col="award_id",
        ),
        "obligated_amount_from_COVID-19_supplementals": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            def_codes=def_codes,
            award_id_col="award_id",
        ),
        "outlayed_amount_from_IIJA_supplemental": _outlay_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            def_codes=def_codes,
            award_id_col="award_id",
        ),
        "obligated_amount_from_IIJA_supplemental": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            def_codes=def_codes,
            award_id_col="award_id",
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), object_class_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), program_activity_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "award_latest_action_date_fiscal_year": FiscalYear(
            F(f"latest_transaction_search__{NORM_TO_TRANSACTION_SEARCH_COL_MAP['action_date']}")
        ),
        "cfda_numbers_and_titles": CFDAs("cfdas"),
    }
    annotation_fields.update(AWARD_SEARCH_CD_DISPLAY_ANNOTATIONS)
    return annotation_fields


def idv_order_annotations(filters: dict, file_type: str = None):
    """
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values
    """
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(
                value=StringAggWithDefault(
                    "treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": ConcatAll(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes"
        + NAMING_CONFLICT_DISCRIMINATOR: _disaster_emergency_fund_codes(award_id_col="award_id"),
        "outlayed_amount_from_COVID-19_supplementals": _outlay_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            award_id_col="award_id",
        ),
        "obligated_amount_from_COVID-19_supplementals": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="covid_19",
            award_id_col="award_id",
        ),
        "outlayed_amount_from_IIJA_supplemental": _outlay_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            award_id_col="award_id",
        ),
        "obligated_amount_from_IIJA_supplemental": _obligation_txn_amount_agg_subquery(
            emergency_fund_group_name="infrastructure",
            award_id_col="award_id",
        ),
        "award_latest_action_date_fiscal_year": FiscalYear(
            F(f"latest_transaction_search__{NORM_TO_TRANSACTION_SEARCH_COL_MAP['action_date']}")
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(filter_limit_to_closed_periods(), award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), program_activity_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
    }
    annotation_fields.update(AWARD_SEARCH_CD_DISPLAY_ANNOTATIONS)
    return annotation_fields


def idv_transaction_annotations(filters: dict, file_type: str = None):
    """
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values
    """
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .values("award_id")
            .annotate(
                value=StringAggWithDefault(
                    "treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": ConcatAll(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=_disaster_emergency_fund_codes(),
            ),
            output_field=TextField(),
        ),
        "outlayed_amount_from_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte=COVID_19_PERIOD_START,
                then=_outlay_amount_agg_subquery(emergency_fund_group_name="covid_19"),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "obligated_amount_from_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte=COVID_19_PERIOD_START,
                then=_obligation_txn_amount_agg_subquery(emergency_fund_group_name="covid_19"),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "outlayed_amount_from_IIJA_supplemental_for_overall_award": Case(
            When(
                action_date__gte=IIJA_PERIOD_START,
                then=_outlay_amount_agg_subquery(
                    emergency_fund_group_name="infrastructure",
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "obligated_amount_from_IIJA_supplemental_for_overall_award": Case(
            When(
                action_date__gte=IIJA_PERIOD_START,
                then=_obligation_txn_amount_agg_subquery(
                    emergency_fund_group_name="infrastructure",
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "total_outlayed_amount_for_overall_award": _outlay_amount_agg_subquery_no_coalesce(award_id_col="award_id"),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), object_class_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), program_activity_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "prime_award_summary_recipient_cd_original": congressional_district_display_name(
            "award__latest_transaction_search__recipient_location_state_code",
            "award__latest_transaction_search__recipient_location_congressional_code",
        ),
        "prime_award_summary_recipient_cd_current": congressional_district_display_name(
            "award__latest_transaction_search__recipient_location_state_code",
            "award__latest_transaction_search__recipient_location_congressional_code_current",
        ),
        "prime_award_summary_place_of_performance_cd_original": congressional_district_display_name(
            "award__latest_transaction_search__pop_state_code",
            "award__latest_transaction_search__pop_congressional_code",
        ),
        "prime_award_summary_place_of_performance_cd_current": congressional_district_display_name(
            "award__latest_transaction_search__pop_state_code",
            "award__latest_transaction_search__pop_congressional_code_current",
        ),
    }
    annotation_fields.update(TXN_SEARCH_CD_DISPLAY_ANNOTATIONS)

    return annotation_fields


def subaward_annotations(filters: dict, file_type: str = None):
    """
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values
    """
    def_codes = filters.get("def_codes", [])
    annotation_fields = {
        "subaward_action_date_fiscal_year": FiscalYear("sub_action_date"),
        "prime_award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "prime_award_period_of_performance_potential_end_date": Cast(
            F("latest_transaction__period_of_perf_potential_e"), DateField()
        ),
        "prime_award_treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAggWithDefault(
                    "awardsearch__financial_set__treasury_account__tas_rendering_label", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "prime_award_federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAggWithDefault(
                    "awardsearch__financial_set__treasury_account__federal_account__federal_account_code",
                    ";",
                    distinct=True,
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": ConcatAll(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "prime_award_object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), object_class_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "prime_award_program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("award_id"), program_activity_id__isnull=False
            )
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAggWithDefault("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "prime_award_disaster_emergency_fund_codes": _disaster_emergency_fund_codes(def_codes=def_codes),
        "prime_award_outlayed_amount_from_COVID-19_supplementals": Case(
            When(
                sub_action_date__gte=COVID_19_PERIOD_START,
                then=_outlay_amount_agg_subquery(
                    emergency_fund_group_name="covid_19",
                    def_codes=def_codes,
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_obligated_amount_from_COVID-19_supplementals": Case(
            When(
                sub_action_date__gte=COVID_19_PERIOD_START,
                then=_obligation_txn_amount_agg_subquery(
                    emergency_fund_group_name="covid_19",
                    def_codes=def_codes,
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_outlayed_amount_from_IIJA_supplemental": Case(
            When(
                sub_action_date__gte=IIJA_PERIOD_START,
                then=_outlay_amount_agg_subquery(
                    emergency_fund_group_name="infrastructure",
                    def_codes=def_codes,
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_obligated_amount_from_IIJA_supplemental": Case(
            When(
                sub_action_date__gte=IIJA_PERIOD_START,
                then=_obligation_txn_amount_agg_subquery(
                    emergency_fund_group_name="infrastructure",
                    def_codes=def_codes,
                ),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_total_outlayed_amount": _outlay_amount_agg_subquery_no_coalesce(award_id_col="award_id"),
        "prime_award_latest_action_date_fiscal_year": FiscalYear("latest_transaction__action_date"),
        "prime_award_cfda_numbers_and_titles": CFDAs("award__cfdas"),
        "prime_award_summary_recipient_cd_original": congressional_district_display_name(
            "legal_entity_state_code",
            "legal_entity_congressional",
        ),
        "prime_award_summary_recipient_cd_current": congressional_district_display_name(
            "legal_entity_state_code",
            "legal_entity_congressional_current",
        ),
        "prime_award_summary_place_of_performance_cd_original": congressional_district_display_name(
            "place_of_perform_state_code",
            "place_of_perform_congressio",
        ),
        "prime_award_summary_place_of_performance_cd_current": congressional_district_display_name(
            "place_of_perform_state_code",
            "place_of_performance_congressional_current",
        ),
    }

    # Subawards populate the same field with different values depending
    # on whether the file type is d1 or d2
    if file_type == "d1":
        annotation_fields["subaward_recipient_cd_original"] = congressional_district_display_name(
            "sub_legal_entity_state_code",
            "sub_legal_entity_congressional",
        )
        annotation_fields["subaward_recipient_cd_current"] = congressional_district_display_name(
            "sub_legal_entity_state_code",
            "sub_legal_entity_congressional_current",
        )
        annotation_fields["subaward_place_of_performance_cd_original"] = congressional_district_display_name(
            "sub_place_of_perform_state_code",
            "sub_place_of_perform_congressio",
        )
        annotation_fields["subaward_place_of_performance_cd_current"] = congressional_district_display_name(
            "sub_place_of_perform_state_code",
            "sub_place_of_performance_congressional_current",
        )
    elif file_type == "d2":
        annotation_fields["subaward_recipient_cd_original"] = congressional_district_display_name(
            "sub_legal_entity_state_code",
            "sub_legal_entity_congressional_raw",
        )
        annotation_fields["subaward_recipient_cd_current"] = congressional_district_display_name(
            "sub_legal_entity_state_code",
            "sub_legal_entity_congressional_current",
        )
        annotation_fields["subaward_place_of_performance_cd_original"] = congressional_district_display_name(
            "sub_place_of_perform_state_code",
            "sub_place_of_perform_congressio_raw",
        )
        annotation_fields["subaward_place_of_performance_cd_current"] = congressional_district_display_name(
            "sub_place_of_perform_state_code",
            "sub_place_of_performance_congressional_current",
        )
    else:
        if file_type is not None:
            raise InvalidParameterException('Invalid Parameter: file_type must be either "d1" or "d2" or None')
    return annotation_fields


def object_class_program_activity_annotations(filters: dict, file_type: str = None):
    """
    These fields are annotated here to avoid them being added to the GROUP BY clause generated by Django, which would
        prevent the TAS from collapsing down to FA correctly
    Args:
        file_type: Either "d1" or "d2". Required because "d1" and "d2"
        sometimes populate the same field with different values.
    """

    annotation_fields = {
        "deobligations_or_recoveries_or_refunds_from_prior_year": Sum(
            "deobligations_recoveries_refund_pri_program_object_class_cpe"
        ),
        "gross_outlay_amount_FYB": Sum("gross_outlay_amount_by_program_object_class_fyb"),
        "gross_outlays_delivered_orders_paid_total": Sum("gross_outlays_delivered_orders_paid_total_cpe"),
        "gross_outlays_delivered_orders_paid_total_FYB": Sum("gross_outlays_delivered_orders_paid_total_fyb"),
        "gross_outlays_undelivered_orders_prepaid_total": Sum("gross_outlays_undelivered_orders_prepaid_total_cpe"),
        "gross_outlays_undelivered_orders_prepaid_total_FYB": Sum("gross_outlays_undelivered_orders_prepaid_total_cpe"),
        "obligations_delivered_orders_unpaid_total": Sum("obligations_delivered_orders_unpaid_total_cpe"),
        "obligations_delivered_orders_unpaid_total_FYB": Sum("obligations_delivered_orders_unpaid_total_cpe"),
        "obligations_undelivered_orders_unpaid_total": Sum("obligations_undelivered_orders_unpaid_total_cpe"),
        "obligations_undelivered_orders_unpaid_total_FYB": Sum("obligations_undelivered_orders_unpaid_total_fyb"),
        "USSGL480100_undelivered_orders_obligations_unpaid": Sum(
            "ussgl480100_undelivered_orders_obligations_unpaid_cpe"
        ),
        "USSGL480100_undelivered_orders_obligations_unpaid_FYB": Sum(
            "ussgl480100_undelivered_orders_obligations_unpaid_fyb"
        ),
        "USSGL480200_undelivered_orders_obligations_prepaid_advanced": Sum(
            "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe"
        ),
        "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB": Sum(
            "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb"
        ),
        "USSGL483100_undelivered_orders_obligations_transferred_unpaid": Sum(
            "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe"
        ),
        "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced": Sum(
            "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe"
        ),
        "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig": Sum(
            "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe"
        ),
        "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid": Sum(
            "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe"
        ),
        "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid": Sum(
            "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe"
        ),
        "USSGL490100_delivered_orders_obligations_unpaid": Sum("ussgl490100_delivered_orders_obligations_unpaid_cpe"),
        "USSGL490100_delivered_orders_obligations_unpaid_FYB": Sum(
            "ussgl490100_delivered_orders_obligations_unpaid_fyb"
        ),
        "USSGL490200_delivered_orders_obligations_paid": Sum("ussgl490200_delivered_orders_obligations_paid_cpe"),
        "USSGL490800_authority_outlayed_not_yet_disbursed": Sum("ussgl490800_authority_outlayed_not_yet_disbursed_cpe"),
        "USSGL490800_authority_outlayed_not_yet_disbursed_FYB": Sum(
            "ussgl490800_authority_outlayed_not_yet_disbursed_fyb"
        ),
        "USSGL493100_delivered_orders_obligations_transferred_unpaid": Sum(
            "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe"
        ),
        "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig": Sum(
            "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe"
        ),
        "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid": Sum(
            "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe"
        ),
        "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid": Sum(
            "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe"
        ),
    }
    return annotation_fields
