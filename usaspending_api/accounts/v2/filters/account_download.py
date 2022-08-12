"""
Account Download Logic

Account Balances (A file):
    - Treasury Account
        1. Get all rows matching the filters for the FYQ/FYP requested
        2. Group by Treasury Account
    - Federal Account
        1. Get all rows matching the filters for the FYQ/FYP requested
        2. Group by Federal Account
Account Breakdown by Program Activity & Object Class (B file):
    - Treasury Account
        1. Get all rows matching the filters for the FYQ/FYP requested
        2. Group by Treasury Account/Program Activity/Object Class/Direct Reimbursable/DEF Code
    - Federal Account
        1. Get all rows matching the filters for the FYQ/FYP requested
        2. Group by Federal Account/Program Activity/Object Class/Direct Reimbursable/DEF Code
Account Breakdown by Award (C file):
    - Treasury Account
        1. Get all rows matching the filters for the FYQ/FYP requested and prior PYQ/PYP in the
           same FY that have TOA != 0
        2. There is no grouping (well, maybe a little bit is used to collapse down reporting
           agencies and budget functions/sub-functions)
    - Federal Account
        1. Get all rows matching the filters for the FYQ/FYP requested and prior PYQ/PYP in the
           same FY that have TOA != 0
        2. Group by Federal Account
"""
from datetime import timezone, datetime

from django.db.models import (
    Case,
    DateField,
    DecimalField,
    F,
    Func,
    Max,
    Q,
    Subquery,
    Sum,
    TextField,
    Value,
    When,
    OuterRef,
    Exists,
)
from django.db.models.functions import Cast, Coalesce, Concat
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import (
    ConcatAll,
    FiscalYear,
    get_fyp_or_q_notation,
    get_gtas_fyp_notation,
    StringAggWithDefault,
)
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import ToptierAgency, CGAC
from usaspending_api.settings import HOST
from usaspending_api.submissions.helpers import (
    ClosedPeriod,
    get_submission_ids_for_periods,
    get_last_closed_periods_per_year,
)
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

AWARD_URL = f"{HOST}/award/" if "localhost" in HOST else f"https://{HOST}/award/"


def account_download_filter(account_type, download_table, filters, account_level="treasury_account"):

    query_filters, tas_id = build_query_filters(account_type, filters, account_level)

    nonzero_filter = Q()
    if account_type == "award_financial":
        nonzero_filter = get_nonzero_filter()

    # Make derivations based on the account level
    if account_level == "treasury_account":
        queryset = generate_treasury_account_query(download_table.objects, account_type)
    elif account_level == "federal_account":
        queryset = generate_federal_account_query(download_table.objects, account_type, tas_id, filters)
    else:
        raise InvalidParameterException(
            'Invalid Parameter: account_level must be either "federal_account" or "treasury_account"'
        )

    if filters.get("is_multi_year"):
        if account_type == "gtas_balances":
            queryset = queryset.filter(Exists(get_gtas_submission_filter()))
        else:
            submission_filter = Q(submission__is_final_balances_for_fy=True)
            queryset = queryset.filter(submission_filter)
    else:
        submission_filter = get_submission_filter(account_type, filters)
        queryset = queryset.filter(submission_filter)

    # Apply filter and return
    return queryset.filter(nonzero_filter, **query_filters)


def build_query_filters(account_type, filters, account_level):
    if account_level not in ("treasury_account", "federal_account"):
        raise InvalidParameterException(
            'Invalid Parameter: account_level must be either "federal_account" or "treasury_account"'
        )

    query_filters = {}

    tas_id = (
        "treasury_account_identifier" if account_type in ("account_balances", "gtas_balances") else "treasury_account"
    )

    if filters.get("agency") and filters["agency"] != "all":
        if not ToptierAgency.objects.filter(toptier_agency_id=filters["agency"]).exists():
            raise InvalidParameterException("Agency with that ID does not exist")
        query_filters[f"{tas_id}__funding_toptier_agency_id"] = filters["agency"]

    if filters.get("federal_account") and filters["federal_account"] != "all":
        if not FederalAccount.objects.filter(id=filters["federal_account"]).exists():
            raise InvalidParameterException("Federal Account with that ID does not exist")
        query_filters[f"{tas_id}__federal_account__id"] = filters["federal_account"]

    if filters.get("budget_function") and filters["budget_function"] != "all":
        query_filters[f"{tas_id}__budget_function_code"] = filters["budget_function"]

    if filters.get("budget_subfunction") and filters["budget_subfunction"] != "all":
        query_filters[f"{tas_id}__budget_subfunction_code"] = filters["budget_subfunction"]

    if account_type != "account_balances":  # file A does not have DEFC field so we do not attempt to filter
        if len(filters.get("def_codes") or []) > 0:
            # joining to disaster_emergency_fund_code table for observed performance benefits
            query_filters["disaster_emergency_fund__code__in"] = filters["def_codes"]

    return query_filters, tas_id


def get_gtas_submission_filter():
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            submission_reveal_date__lte=datetime.now(timezone.utc), is_quarter=False
        )
        .values("submission_fiscal_year", "is_quarter")
        .annotate(max_submission_fiscal_month=Max("submission_fiscal_month"))
        .filter(submission_fiscal_year=OuterRef("fiscal_year"), max_submission_fiscal_month=OuterRef("fiscal_period"))
    )


def get_submission_filter(account_type, filters):
    """
    Limits the overall File A, B, and C submissions that are looked at.
    For File A and B we only look at the most recent submissions for
    the provided filters, because these files' dollar amounts are
    year-to-date cumulative balances. For File C we expand this to
    include all submissions up to the provided filters, so that we can
    get the incremental `transaction_obligated_amount` from each
    period in the time frame, in addition to the latest periods' cumulative
    balance.
    """
    filter_year = int(filters.get("fy") or -1)
    filter_quarter = int(filters.get("quarter") or -1)
    filter_month = int(filters.get("period") or -1)

    submission_ids = get_submission_ids_for_periods(filter_year, filter_quarter, filter_month)
    if submission_ids:
        submission_id_filter = Q(submission_id__in=submission_ids)
    else:
        submission_id_filter = Q(submission_id__isnull=True)

    if account_type in ["account_balances", "object_class_program_activity"]:
        submission_filter = submission_id_filter

    else:
        # For File C, we want:
        #   - outlays in the most recent agency submission period matching the filter criteria
        #   - obligations in any period matching the filter criteria or earlier
        # Specific filtering to limit outlays to most recent submission period can be found
        # with the outlay related fields
        submission_date_filter = Q(
            Q(
                Q(Q(submission__reporting_fiscal_period__lte=filter_month) & Q(submission__quarter_format_flag=False))
                | Q(
                    Q(submission__reporting_fiscal_quarter__lte=filter_quarter)
                    & Q(submission__quarter_format_flag=True)
                )
            )
            & Q(submission__reporting_fiscal_year=filter_year)
        )

        submission_filter = submission_id_filter | submission_date_filter

    return submission_filter


def get_nonzero_filter():
    nonzero_outlay = Q(
        Q(gross_outlay_amount_FYB_to_period_end__gt=0)
        | Q(gross_outlay_amount_FYB_to_period_end__lt=0)
        | Q(USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig__gt=0)
        | Q(USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig__lt=0)
        | Q(USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig__gt=0)
        | Q(USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig__lt=0)
    )
    nonzero_toa = Q(Q(transaction_obligated_amount__gt=0) | Q(transaction_obligated_amount__lt=0))
    return nonzero_outlay | nonzero_toa


def _build_submission_queryset(closed_period: ClosedPeriod):
    if closed_period.is_final:
        q = closed_period.build_period_q("submission")
    else:
        q = closed_period.build_submission_id_q("submission")
    return q


def build_queryset_for_closed_submissions(filters):
    filter_year = filters.get("fy")

    q = Q()
    if filter_year:
        selected_period = ClosedPeriod(filter_year, filters.get("quarter"), filters.get("period"))
        q = _build_submission_queryset(selected_period)
    else:
        closed_periods = get_last_closed_periods_per_year()
        for closed_period in closed_periods:
            q |= _build_submission_queryset(closed_period)

    return q


def _build_submission_queryset_for_derived_fields(submission_closed_period_queryset, column_name):
    if submission_closed_period_queryset:
        queryset = Case(
            When(submission_closed_period_queryset, then=F(column_name)),
            default=Cast(Value(None), DecimalField(max_digits=23, decimal_places=2)),
            output_field=DecimalField(max_digits=23, decimal_places=2),
        )
    else:
        queryset = F(column_name)
    return queryset


def generate_ussgl487200_derived_field(submission_queryset=None):
    column_name = "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe"
    return _build_submission_queryset_for_derived_fields(submission_queryset, column_name)


def generate_ussgl497200_derived_field(submission_queryset=None):
    column_name = "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"
    return _build_submission_queryset_for_derived_fields(submission_queryset, column_name)


def generate_gross_outlay_amount_derived_field(account_type, submission_queryset=None):
    column_name = {
        "account_balances": "gross_outlay_amount_by_tas_cpe",
        "gtas_balances": "gross_outlay_amount_by_tas_cpe",
        "object_class_program_activity": "gross_outlay_amount_by_program_object_class_cpe",
        "award_financial": "gross_outlay_amount_by_award_cpe",
    }[account_type]

    return _build_submission_queryset_for_derived_fields(submission_queryset, column_name)


def generate_treasury_account_query(queryset, account_type):
    """Derive necessary fields for a treasury account-grouped query"""
    derived_fields = {
        "submission_period": get_fyp_or_q_notation("submission"),
        "gross_outlay_amount": generate_gross_outlay_amount_derived_field(account_type),
        "gross_outlay_amount_FYB_to_period_end": generate_gross_outlay_amount_derived_field(account_type),
    }

    lmd = "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR

    if account_type == "gtas_balances":
        derived_fields = gtas_balances_derivations(derived_fields)
        derived_fields.update(
            {
                "submission_period": get_gtas_fyp_notation(),
            }
        )

    if account_type not in ("account_balances", "gtas_balances"):
        derived_fields.update(
            {
                "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": generate_ussgl487200_derived_field(),
                "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": generate_ussgl497200_derived_field(),
            }
        )

    if account_type == "award_financial":
        # Separating out last_modified_date like this prevents unnecessary grouping in the full File
        # C TAS download.  Keeping it as MAX caused grouping on every single column in the SQL statement.
        derived_fields[lmd] = Cast("submission__published_date", output_field=DateField())
        derived_fields = award_financial_derivations(derived_fields)
    elif account_type != "gtas_balances":
        derived_fields[lmd] = Cast(Max("submission__published_date"), output_field=DateField())

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id, filters):
    """Group by federal account (and budget function/subfunction) and SUM all other fields"""
    # Submission Queryset is only built for Federal Account downloads since the TAS are rolled up into
    # the Federal Account. For cases such as Treasury Account download where there is no GROUP BY in
    # the resulting SQL query this is not needed.
    closed_submission_queryset = build_queryset_for_closed_submissions(filters)
    derived_fields = {
        "reporting_agency_name": StringAggWithDefault("submission__reporting_agency_name", "; ", distinct=True),
        "budget_function": StringAggWithDefault(f"{tas_id}__budget_function_title", "; ", distinct=True),
        "budget_subfunction": StringAggWithDefault(f"{tas_id}__budget_subfunction_title", "; ", distinct=True),
        "submission_period": get_fyp_or_q_notation("submission"),
        "last_modified_date"
        + NAMING_CONFLICT_DISCRIMINATOR: Cast(Max("submission__published_date"), output_field=DateField()),
        "gross_outlay_amount": Sum(
            generate_gross_outlay_amount_derived_field(account_type, closed_submission_queryset)
        ),
        "gross_outlay_amount_FYB_to_period_end": Sum(
            generate_gross_outlay_amount_derived_field(account_type, closed_submission_queryset)
        ),
    }

    if account_type != "account_balances":
        derived_fields.update(
            {
                "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": Sum(
                    generate_ussgl487200_derived_field(closed_submission_queryset)
                ),
                "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": Sum(
                    generate_ussgl497200_derived_field(closed_submission_queryset)
                ),
            }
        )
    if account_type == "award_financial":
        derived_fields = award_financial_derivations(derived_fields)

    queryset = queryset.annotate(**derived_fields)

    # List of all columns that may appear in A, B, or C files that can be summed
    all_summed_cols = [
        "budget_authority_unobligated_balance_brought_forward",
        "adjustments_to_unobligated_balance_brought_forward",
        "budget_authority_appropriated_amount",
        "borrowing_authority_amount",
        "contract_authority_amount",
        "spending_authority_from_offsetting_collections_amount",
        "total_other_budgetary_resources_amount",
        "total_budgetary_resources",
        "obligations_incurred",
        "deobligations_or_recoveries_or_refunds_from_prior_year",
        "unobligated_balance",
        "status_of_budgetary_resources_total",
        "transaction_obligated_amount",
    ]

    # Group by all columns within the file that can't be summed
    fed_acct_values_dict = query_paths[account_type]["federal_account"]
    grouped_cols = [fed_acct_values_dict[val] for val in fed_acct_values_dict if val not in all_summed_cols]
    queryset = queryset.values(*grouped_cols)

    # Sum all fields from all_summed_cols that appear in this file
    values_dict = query_paths[account_type]
    summed_cols = {
        val: Sum(values_dict["treasury_account"].get(val, None))
        for val in values_dict["federal_account"]
        if val in all_summed_cols
    }

    return queryset.annotate(**summed_cols)


def award_financial_derivations(derived_fields):
    derived_fields["award_type_code"] = Coalesce(
        "award__latest_transaction__contract_data__contract_award_type",
        "award__latest_transaction__assistance_data__assistance_type",
        output_field=TextField(),
    )
    derived_fields["award_type"] = Coalesce(
        "award__latest_transaction__contract_data__contract_award_type_desc",
        "award__latest_transaction__assistance_data__assistance_type_desc",
        output_field=TextField(),
    )
    derived_fields["awarding_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_agency_code",
        "award__latest_transaction__assistance_data__awarding_agency_code",
        output_field=TextField(),
    )
    derived_fields["awarding_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_agency_name",
        "award__latest_transaction__assistance_data__awarding_agency_name",
        output_field=TextField(),
    )
    derived_fields["awarding_subagency_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_sub_tier_agency_c",
        "award__latest_transaction__assistance_data__awarding_sub_tier_agency_c",
        output_field=TextField(),
    )
    derived_fields["awarding_subagency_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_sub_tier_agency_n",
        "award__latest_transaction__assistance_data__awarding_sub_tier_agency_n",
        output_field=TextField(),
    )
    derived_fields["awarding_office_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_office_code",
        "award__latest_transaction__assistance_data__awarding_office_code",
        output_field=TextField(),
    )
    derived_fields["awarding_office_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_office_name",
        "award__latest_transaction__assistance_data__awarding_office_name",
        output_field=TextField(),
    )
    derived_fields["funding_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_agency_code",
        "award__latest_transaction__assistance_data__funding_agency_code",
        output_field=TextField(),
    )
    derived_fields["funding_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_agency_name",
        "award__latest_transaction__assistance_data__funding_agency_name",
        output_field=TextField(),
    )
    derived_fields["funding_sub_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_sub_tier_agency_co",
        "award__latest_transaction__assistance_data__funding_sub_tier_agency_co",
        output_field=TextField(),
    )
    derived_fields["funding_sub_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_sub_tier_agency_na",
        "award__latest_transaction__assistance_data__funding_sub_tier_agency_na",
        output_field=TextField(),
    )
    derived_fields["funding_office_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_office_code",
        "award__latest_transaction__assistance_data__funding_office_code",
        output_field=TextField(),
    )
    derived_fields["funding_office_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_office_name",
        "award__latest_transaction__assistance_data__funding_office_name",
        output_field=TextField(),
    )
    derived_fields["recipient_duns"] = Coalesce(
        "award__latest_transaction__contract_data__awardee_or_recipient_uniqu",
        "award__latest_transaction__assistance_data__awardee_or_recipient_uniqu",
        output_field=TextField(),
    )
    derived_fields["recipient_name"] = Coalesce(
        "award__latest_transaction__contract_data__awardee_or_recipient_legal",
        "award__latest_transaction__assistance_data__awardee_or_recipient_legal",
        output_field=TextField(),
    )
    derived_fields["recipient_uei"] = Coalesce(
        "award__latest_transaction__contract_data__awardee_or_recipient_uei",
        "award__latest_transaction__assistance_data__uei",
        output_field=TextField(),
    )
    derived_fields["recipient_parent_duns"] = Coalesce(
        "award__latest_transaction__contract_data__ultimate_parent_unique_ide",
        "award__latest_transaction__assistance_data__ultimate_parent_unique_ide",
        output_field=TextField(),
    )
    derived_fields["recipient_parent_name"] = Coalesce(
        "award__latest_transaction__contract_data__ultimate_parent_legal_enti",
        "award__latest_transaction__assistance_data__ultimate_parent_legal_enti",
        output_field=TextField(),
    )
    derived_fields["recipient_parent_uei"] = Coalesce(
        "award__latest_transaction__contract_data__ultimate_parent_uei",
        "award__latest_transaction__assistance_data__ultimate_parent_uei",
        output_field=TextField(),
    )
    derived_fields["recipient_country"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_country_code",
        "award__latest_transaction__assistance_data__legal_entity_country_code",
        output_field=TextField(),
    )
    derived_fields["recipient_state"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_state_code",
        "award__latest_transaction__assistance_data__legal_entity_state_code",
        output_field=TextField(),
    )
    derived_fields["recipient_county"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_county_name",
        "award__latest_transaction__assistance_data__legal_entity_county_name",
        output_field=TextField(),
    )
    derived_fields["recipient_city"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_city_name",
        "award__latest_transaction__assistance_data__legal_entity_city_name",
        output_field=TextField(),
    )
    derived_fields["recipient_congressional_district"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_congressional",
        "award__latest_transaction__assistance_data__legal_entity_congressional",
        output_field=TextField(),
    )
    derived_fields["recipient_zip_code"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_zip4",
        Concat(
            "award__latest_transaction__assistance_data__legal_entity_zip5",
            "award__latest_transaction__assistance_data__legal_entity_zip_last4",
        ),
        output_field=TextField(),
    )
    derived_fields["primary_place_of_performance_country"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perf_country_desc",
        "award__latest_transaction__assistance_data__place_of_perform_country_n",
        output_field=TextField(),
    )
    derived_fields["primary_place_of_performance_state"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perfor_state_desc",
        "award__latest_transaction__assistance_data__place_of_perform_state_nam",
        output_field=TextField(),
    )
    derived_fields["primary_place_of_performance_county"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perform_county_na",
        "award__latest_transaction__assistance_data__place_of_perform_county_na",
        output_field=TextField(),
    )
    derived_fields["primary_place_of_performance_congressional_district"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_performance_congr",
        "award__latest_transaction__assistance_data__place_of_performance_congr",
        output_field=TextField(),
    )
    derived_fields["primary_place_of_performance_zip_code"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_performance_zip4a",
        "award__latest_transaction__assistance_data__place_of_performance_zip4a",
        output_field=TextField(),
    )
    derived_fields["award_base_action_date_fiscal_year"] = FiscalYear("award__date_signed")
    derived_fields["award_latest_action_date_fiscal_year"] = FiscalYear("award__certified_date")
    derived_fields["usaspending_permalink"] = Case(
        When(
            **{
                "award__generated_unique_award_id__isnull": False,
                "then": ConcatAll(
                    Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
                ),
            }
        ),
        default=Value(""),
        output_field=TextField(),
    )

    return derived_fields


def gtas_balances_derivations(derived_fields):
    # These derivations are used by the following derivation; however they are NOT included in the final download
    derived_fields["tas_component_count"] = Func(
        Func(F("tas_rendering_label"), Value("-"), function="string_to_array"),
        1,
        function="array_upper",
        output_field=TextField(),
    )
    derived_fields["tas_component_third_from_end"] = Func(
        Func(F("tas_rendering_label"), function="REVERSE"),
        Value("-"),
        Value("3"),
        function="SPLIT_PART",
        output_field=TextField(),
    )

    # These derivations appear in the final download
    derived_fields["allocation_transfer_agency_identifier_code"] = Coalesce(
        F("treasury_account_identifier__allocation_transfer_agency_id"),
        Case(
            When(
                tas_component_count=5, then=Func(F("tas_rendering_label"), Value("-"), Value(1), function="SPLIT_PART")
            ),
            default=None,
            output_field=TextField(),
        ),
    )
    derived_fields["agency_identifier_code"] = Coalesce(
        F("treasury_account_identifier__agency_id"),
        Case(
            When(
                tas_component_count=5,
                then=Func(
                    F("tas_rendering_label"), Value("-"), Value(2), function="SPLIT_PART", output_field=TextField()
                ),
            ),
            default=Func(F("tas_rendering_label"), Value("-"), Value(1), function="SPLIT_PART"),
            output_field=TextField(),
        ),
        output_field=TextField(),
    )
    derived_fields["beginning_period_of_availability"] = Coalesce(
        F("treasury_account_identifier__beginning_period_of_availability"),
        Case(
            When(
                ~Q(tas_component_third_from_end=Value("X")),
                then=Func(
                    Func(
                        Func(
                            Func(F("tas_rendering_label"), function="REVERSE", output_field=TextField()),
                            Value("-"),
                            Value(3),
                            function="SPLIT_PART",
                            output_field=TextField(),
                        ),
                        Value("/"),
                        Value(2),
                        function="SPLIT_PART",
                        output_field=TextField(),
                    ),
                    function="REVERSE",
                    output_field=TextField(),
                ),
            ),
            default=None,
            output_field=TextField(),
        ),
        output_field=TextField(),
    )
    derived_fields["ending_period_of_availability"] = Coalesce(
        F("treasury_account_identifier__ending_period_of_availability"),
        Case(
            When(
                ~Q(tas_component_third_from_end=Value("X")),
                then=Func(
                    Func(
                        Func(
                            Func(F("tas_rendering_label"), function="REVERSE", output_field=TextField()),
                            Value("-"),
                            Value(3),
                            function="SPLIT_PART",
                            output_field=TextField(),
                        ),
                        Value("/"),
                        Value(1),
                        function="SPLIT_PART",
                        output_field=TextField(),
                    ),
                    function="REVERSE",
                    output_field=TextField(),
                ),
            ),
            default=None,
            output_field=TextField(),
        ),
        output_field=TextField(),
    )
    derived_fields["availability_type_code"] = Coalesce(
        F("treasury_account_identifier__availability_type_code"),
        Case(
            When(
                Q(tas_component_third_from_end=Value("X")),
                then=Value("X"),
            ),
            default=None,
            output_field=TextField(),
        ),
        output_field=TextField(),
    )
    derived_fields["main_account_code"] = Func(
        Func(
            Func(F("tas_rendering_label"), function="REVERSE", output_field=TextField()),
            Value("-"),
            Value(2),
            function="SPLIT_PART",
            output_field=TextField(),
        ),
        function="REVERSE",
        output_field=TextField(),
    )
    derived_fields["sub_account_code"] = Func(
        Func(
            Func(F("tas_rendering_label"), function="REVERSE", output_field=TextField()),
            Value("-"),
            Value(1),
            function="SPLIT_PART",
            output_field=TextField(),
        ),
        function="REVERSE",
        output_field=TextField(),
    )
    derived_fields["agency_identifier_name"] = Subquery(
        CGAC.objects.filter(cgac_code=OuterRef("agency_identifier_code")).values("agency_name")
    )
    derived_fields["allocation_transfer_agency_identifier_name"] = Subquery(
        CGAC.objects.filter(cgac_code=OuterRef("allocation_transfer_agency_identifier_code")).values("agency_name")
    )

    return derived_fields
