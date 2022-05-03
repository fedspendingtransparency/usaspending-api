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
from usaspending_api.common.helpers.orm_helpers import ConcatAll, FiscalYear, StringAggWithDefault
from usaspending_api.awards.models import Award, FinancialAccountsByAwards, TransactionFABS
from usaspending_api.disaster.v2.views.disaster_base import (
    filter_by_latest_closed_periods,
    final_submissions_for_all_fy,
)
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.settings import HOST


AWARD_URL = f"{HOST}/award/" if "localhost" in HOST else f"https://{HOST}/award/"


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


def _covid_outlay_subquery(def_codes: Optional[List[str]] = None, award_id_col: Optional[str] = "award_id") -> Subquery:
    filters = [
        filter_by_latest_closed_periods(),
        Q(award_id=OuterRef(award_id_col)),
        Q(submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1))),
        Q(disaster_emergency_fund__group_name="covid_19"),
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


def _covid_obligation_subquery(
    def_codes: Optional[List[str]] = None, award_id_col: Optional[str] = "award_id"
) -> Subquery:
    filters = [
        filter_limit_to_closed_periods(),
        Q(award_id=OuterRef(award_id_col)),
        Q(submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1))),
        Q(disaster_emergency_fund__group_name="covid_19"),
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


def transaction_search_annotations(filters: dict):
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
            Value(AWARD_URL), Func(F("transaction__award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=_disaster_emergency_fund_codes(def_codes=def_codes),
            ),
            output_field=TextField(),
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_outlay_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_obligation_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
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
    }
    return annotation_fields


def transaction_annotations(filters: dict):
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
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                action_date__gte=datetime.date(2020, 4, 1),
                then=_disaster_emergency_fund_codes(def_codes=def_codes),
            ),
            output_field=TextField(),
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_outlay_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_obligation_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
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
    }
    return annotation_fields


def award_annotations(filters: dict):
    def_codes = filters.get("def_codes", [])
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
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
        + NAMING_CONFLICT_DISCRIMINATOR: _disaster_emergency_fund_codes(def_codes=def_codes, award_id_col="id"),
        "outlayed_amount_funded_by_COVID-19_supplementals": _covid_outlay_subquery(
            def_codes=def_codes, award_id_col="id"
        ),
        "obligated_amount_funded_by_COVID-19_supplementals": _covid_obligation_subquery(
            def_codes=def_codes, award_id_col="id"
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_limit_to_closed_periods(), award_id=OuterRef("id"), object_class_id__isnull=False
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
                filter_limit_to_closed_periods(), award_id=OuterRef("id"), program_activity_id__isnull=False
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
        "award_latest_action_date_fiscal_year": FiscalYear(F("latest_transaction__action_date")),
        "cfda_numbers_and_titles": Subquery(
            TransactionFABS.objects.filter(transaction__award_id=OuterRef("id"))
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("cfda_number"), Value(": "), F("cfda_title")),
                    output_field=TextField(),
                ),
            )
            .values("transaction__award_id")
            .annotate(total=StringAggWithDefault("value", "; ", distinct=True, ordering="value"))
            .values("total"),
            output_field=TextField(),
        ),
    }
    return annotation_fields


def idv_order_annotations(filters: dict):
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
            .values("award_id")
            .annotate(value=StringAggWithDefault("treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
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
        + NAMING_CONFLICT_DISCRIMINATOR: _disaster_emergency_fund_codes(award_id_col="id"),
        "outlayed_amount_funded_by_COVID-19_supplementals": _covid_outlay_subquery(award_id_col="id"),
        "obligated_amount_funded_by_COVID-19_supplementals": _covid_obligation_subquery(award_id_col="id"),
        "award_latest_action_date_fiscal_year": FiscalYear(F("latest_transaction__action_date")),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(filter_limit_to_closed_periods(), award_id=OuterRef("id"))
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
                filter_limit_to_closed_periods(), award_id=OuterRef("id"), program_activity_id__isnull=False
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
    return annotation_fields


def idv_transaction_annotations(filters: dict):
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
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=_disaster_emergency_fund_codes(),
            ),
            output_field=TextField(),
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=_covid_outlay_subquery(),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=_covid_obligation_subquery(),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
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
    }
    return annotation_fields


def subaward_annotations(filters: dict):
    def_codes = filters.get("def_codes", [])
    annotation_fields = {
        "subaward_action_date_fiscal_year": FiscalYear("subaward__action_date"),
        "prime_award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "prime_award_period_of_performance_potential_end_date": Cast(
            F("award__latest_transaction__contract_data__period_of_perf_potential_e"), DateField()
        ),
        "prime_award_treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAggWithDefault("financial_set__treasury_account__tas_rendering_label", ";", distinct=True)
            )
            .values("value"),
            output_field=TextField(),
        ),
        "prime_award_federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAggWithDefault(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
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
        "prime_award_disaster_emergency_fund_codes": Case(
            When(
                broker_subaward__sub_action_date__gte=datetime.date(2020, 4, 1),
                then=_disaster_emergency_fund_codes(def_codes=def_codes),
            ),
            output_field=TextField(),
        ),
        "prime_award_outlayed_amount_funded_by_COVID-19_supplementals": Case(
            When(
                broker_subaward__sub_action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_outlay_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_obligated_amount_funded_by_COVID-19_supplementals": Case(
            When(
                broker_subaward__sub_action_date__gte=datetime.date(2020, 4, 1),
                then=_covid_obligation_subquery(def_codes=def_codes),
            ),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        ),
        "prime_award_latest_action_date_fiscal_year": FiscalYear("award__latest_transaction__action_date"),
        "prime_award_cfda_numbers_and_titles": Subquery(
            TransactionFABS.objects.filter(transaction__award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    ConcatAll(F("cfda_number"), Value(": "), F("cfda_title")),
                    output_field=TextField(),
                )
            )
            .values("transaction__award_id")
            .annotate(total=StringAggWithDefault("value", "; ", distinct=True, ordering="value"))
            .values("total"),
            output_field=TextField(),
        ),
    }
    return annotation_fields
