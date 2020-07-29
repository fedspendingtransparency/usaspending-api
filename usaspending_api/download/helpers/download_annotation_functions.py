import datetime
from django.contrib.postgres.aggregates import StringAgg

from usaspending_api.common.helpers.orm_helpers import FiscalYear
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import filter_by_latest_closed_periods
from usaspending_api.settings import HOST
from django.db.models.functions import Concat, Cast, Coalesce
from django.db.models import (
    Func,
    F,
    Value,
    Subquery,
    OuterRef,
    TextField,
    DateField,
    ExpressionWrapper,
    Sum,
    DecimalField,
    Case,
    When,
    Count,
    Q,
)

AWARD_URL = f"{HOST}/#/award/" if "localhost" in HOST else f"https://{HOST}/#/award/"


def universal_transaction_matview_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAgg(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("transaction__award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
                    .annotate(
                        value=ExpressionWrapper(
                            Case(
                                When(
                                    disaster_emergency_fund__code__isnull=False,
                                    then=Concat(
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
                    .annotate(total=StringAgg("value", ";", distinct=True))
                    .values("total"),
                    output_field=TextField(),
                ),
            )
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        filter_by_latest_closed_periods(),
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("gross_outlay_amount_by_award_cpe"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                transaction__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("transaction_obligated_amount"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
    }
    return annotation_fields


def universal_award_matview_annotations():
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAgg(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Case(
                        When(
                            disaster_emergency_fund__code__isnull=False,
                            then=Concat(
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
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_by_latest_closed_periods(),
                award_id=OuterRef("award_id"),
                disaster_emergency_fund__group_name="covid_19",
                submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
            )
            .values("award_id")
            .annotate(sum=Sum("gross_outlay_amount_by_award_cpe"))
            .values("sum"),
            output_field=DecimalField(),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals": Subquery(
            FinancialAccountsByAwards.objects.filter(
                award_id=OuterRef("award_id"),
                disaster_emergency_fund__group_name="covid_19",
                submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
            )
            .values("award_id")
            .annotate(sum=Sum("transaction_obligated_amount"))
            .values("sum"),
            output_field=DecimalField(),
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "award_latest_action_date_fiscal_year": FiscalYear(F("award__latest_transaction__action_date")),
    }
    return annotation_fields


def idv_order_annotations():
    annotation_fields = {
        "award_base_action_date_fiscal_year": FiscalYear("date_signed"),
        "treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("id"))
            .annotate(
                value=StringAgg(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
            .annotate(
                value=ExpressionWrapper(
                    Case(
                        When(
                            disaster_emergency_fund__code__isnull=False,
                            then=Concat(
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
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals": Subquery(
            FinancialAccountsByAwards.objects.filter(
                filter_by_latest_closed_periods(),
                award_id=OuterRef("id"),
                disaster_emergency_fund__group_name="covid_19",
                submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
            )
            .values("award_id")
            .annotate(sum=Sum("gross_outlay_amount_by_award_cpe"))
            .values("sum"),
            output_field=DecimalField(),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals": Subquery(
            FinancialAccountsByAwards.objects.filter(
                award_id=OuterRef("id"),
                disaster_emergency_fund__group_name="covid_19",
                submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
            )
            .values("award_id")
            .annotate(sum=Sum("transaction_obligated_amount"))
            .values("sum"),
            output_field=DecimalField(),
        ),
        "award_latest_action_date_fiscal_year": FiscalYear(F("latest_transaction__action_date")),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
    }
    return annotation_fields


def idv_transaction_annotations():
    annotation_fields = {
        "action_date_fiscal_year": FiscalYear("action_date"),
        "treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAgg(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "disaster_emergency_fund_codes_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
                    .annotate(
                        value=ExpressionWrapper(
                            Case(
                                When(
                                    disaster_emergency_fund__code__isnull=False,
                                    then=Concat(
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
                    .annotate(total=StringAgg("value", ";", distinct=True))
                    .values("total"),
                    output_field=TextField(),
                ),
            )
        ),
        "outlayed_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        filter_by_latest_closed_periods(),
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("gross_outlay_amount_by_award_cpe"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "obligated_amount_funded_by_COVID-19_supplementals_for_overall_award": Case(
            When(
                action_date__gte="2020-04-01",
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("transaction_obligated_amount"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
    }
    return annotation_fields


def subaward_annotations():
    annotation_fields = {
        "subaward_action_date_fiscal_year": FiscalYear("subaward__action_date"),
        "prime_award_base_action_date_fiscal_year": FiscalYear("award__date_signed"),
        "period_of_performance_potential_end_date": Cast(
            F("award__latest_transaction__contract_data__period_of_perf_potential_e"), DateField()
        ),
        "prime_award_treasury_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(value=StringAgg("financial_set__treasury_account__tas_rendering_label", ";", distinct=True))
            .values("value"),
            output_field=TextField(),
        ),
        "prime_award_federal_accounts_funding_this_award": Subquery(
            Award.objects.filter(id=OuterRef("award_id"))
            .annotate(
                value=StringAgg(
                    "financial_set__treasury_account__federal_account__federal_account_code", ";", distinct=True
                )
            )
            .values("value"),
            output_field=TextField(),
        ),
        "usaspending_permalink": Concat(
            Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
        ),
        "prime_award_object_classes_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(F("object_class__object_class"), Value(": "), F("object_class__object_class_name")),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "prime_award_program_activities_funding_this_award": Subquery(
            FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
            .annotate(
                value=ExpressionWrapper(
                    Concat(
                        F("program_activity__program_activity_code"),
                        Value(": "),
                        F("program_activity__program_activity_name"),
                    ),
                    output_field=TextField(),
                )
            )
            .values("award_id")
            .annotate(total=StringAgg("value", ";", distinct=True))
            .values("total"),
            output_field=TextField(),
        ),
        "prime_award_disaster_emergency_fund_codes": Case(
            When(
                broker_subaward__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(award_id=OuterRef("award_id"))
                    .annotate(
                        value=ExpressionWrapper(
                            Case(
                                When(
                                    disaster_emergency_fund__code__isnull=False,
                                    then=Concat(
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
                    .annotate(total=StringAgg("value", ";", distinct=True))
                    .values("total"),
                    output_field=TextField(),
                ),
            )
        ),
        "prime_award_outlayed_amount_funded_by_COVID-19_supplementals": Case(
            When(
                broker_subaward__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        filter_by_latest_closed_periods(),
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("gross_outlay_amount_by_award_cpe"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "prime_award_obligated_amount_funded_by_COVID-19_supplementals": Case(
            When(
                broker_subaward__action_date__gte=datetime.date(2020, 4, 1),
                then=Subquery(
                    FinancialAccountsByAwards.objects.filter(
                        award_id=OuterRef("award_id"),
                        disaster_emergency_fund__group_name="covid_19",
                        submission__reporting_period_start__gte=str(datetime.date(2020, 4, 1)),
                    )
                    .values("award_id")
                    .annotate(sum=Sum("transaction_obligated_amount"))
                    .values("sum"),
                    output_field=DecimalField(),
                ),
            ),
        ),
        "prime_award_latest_action_date_fiscal_year": FiscalYear("award__latest_transaction__action_date"),
    }
    return annotation_fields


def disaster_recipient_annotations():
    annotation_fields = {
        "award_obligations": Coalesce(
            Sum(
                "award__financial_set__transaction_obligated_amount",
                filter=Q(award__financial_set__disaster_emergency_fund__group_name="covid_19"),
                output_field=DecimalField(),
            ),
            0,
        ),
        "award_outlays": Coalesce(
            Sum(
                "award__financial_set__gross_outlay_amount_by_award_cpe",
                filter=Q(
                    filter_by_latest_closed_periods("award__financial_set__"),
                    award__financial_set__disaster_emergency_fund__group_name="covid_19",
                ),
                output_field=DecimalField(),
            ),
            0,
        ),
        "face_value_of_loans": Coalesce(Sum("total_loan_value"), 0),
        "number_of_awards": Count("award_id"),
    }

    return annotation_fields
