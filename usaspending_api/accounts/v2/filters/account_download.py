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
from django.contrib.postgres.aggregates import StringAgg
from django.db.models import Case, CharField, DateField, DecimalField, F, Func, Max, Sum, Value, When, Q
from django.db.models.functions import Cast, Coalesce, Concat
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import (
    ConcatAll,
    FiscalYear,
    get_agency_name_annotation,
    get_fyp_or_q_notation,
)
from usaspending_api.download.filestreaming import NAMING_CONFLICT_DISCRIMINATOR
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import ToptierAgency
from usaspending_api.settings import HOST
from usaspending_api.submissions.helpers import (
    ClosedPeriod,
    get_last_closed_periods_per_year,
    get_submission_ids_for_periods,
)

AWARD_URL = f"{HOST}/#/award/" if "localhost" in HOST else f"https://{HOST}/#/award/"


def account_download_filter(account_type, download_table, filters, account_level="treasury_account"):

    if account_level not in ("treasury_account", "federal_account"):
        raise InvalidParameterException(
            'Invalid Parameter: account_level must be either "federal_account" or "treasury_account"'
        )

    query_filters = {}

    tas_id = "treasury_account_identifier" if account_type == "account_balances" else "treasury_account"

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

    submission_filter = get_submission_filter(account_type, filters)

    # Make derivations based on the account level
    if account_level == "treasury_account":
        queryset = generate_treasury_account_query(download_table.objects, account_type, tas_id, filters)
    elif account_level == "federal_account":
        queryset = generate_federal_account_query(download_table.objects, account_type, tas_id, filters)
    else:
        raise InvalidParameterException(
            'Invalid Parameter: account_level must be either "federal_account" or "treasury_account"'
        )

    # Apply filter and return
    return queryset.filter(submission_filter, **query_filters)


def get_submission_filter(account_type, filters):

    filter_year = int(filters.get("fy") or -1)
    filter_quarter = int(filters.get("quarter") or -1)
    filter_month = int(filters.get("period") or -1)

    submission_ids = get_submission_ids_for_periods(filter_year, filter_quarter, filter_month)
    if submission_ids:
        outlay_filter = Q(submission_id__in=submission_ids)
    else:
        outlay_filter = Q(submission_id__isnull=True)

    if account_type in ["account_balances", "object_class_program_activity"]:
        submission_filter = outlay_filter

    else:
        # For File C, we want:
        #   - outlays in the most recent agency submission period matching the filter criteria
        #   - obligations in any period matching the filter criteria or earlier
        obligation_filter = Q(
            Q(
                Q(Q(submission__reporting_fiscal_period__lte=filter_month) & Q(submission__quarter_format_flag=False))
                | Q(
                    Q(submission__reporting_fiscal_quarter__lte=filter_quarter)
                    & Q(submission__quarter_format_flag=True)
                )
            )
            & Q(submission__reporting_fiscal_year=filter_year)
        )

        submission_filter = Q(
            outlay_filter & (Q(gross_outlay_amount_by_award_cpe__gt=0) | Q(gross_outlay_amount_by_award_cpe__lt=0))
        ) | Q(obligation_filter & Q(Q(transaction_obligated_amount__gt=0) | Q(transaction_obligated_amount__lt=0)))

    return submission_filter


def generate_gross_outlay_amount_derived_field(filters, account_type):
    column_name = {
        "account_balances": "gross_outlay_amount_by_tas_cpe",
        "object_class_program_activity": "gross_outlay_amount_by_program_object_class_cpe",
        "award_financial": "gross_outlay_amount_by_award_cpe",
    }[account_type]

    filter_year = filters.get("fy")

    closed_periods = get_last_closed_periods_per_year()

    q = Q()
    for closed_period in closed_periods:
        if closed_period.fiscal_year == filter_year:
            closed_period = ClosedPeriod(filter_year, filters.get("quarter"), filters.get("period"))
        if closed_period.is_final:
            q |= closed_period.build_period_q("submission")
        else:
            q |= closed_period.build_submission_id_q("submission")

    if q:
        return Case(
            When(q, then=F(column_name)), default=Cast(Value(None), DecimalField(max_digits=23, decimal_places=2))
        )

    return Cast(Value(None), DecimalField(max_digits=23, decimal_places=2))


def generate_treasury_account_query(queryset, account_type, tas_id, filters):
    """ Derive necessary fields for a treasury account-grouped query """
    derived_fields = {
        "allocation_transfer_agency_identifier_name": get_agency_name_annotation(
            tas_id, "allocation_transfer_agency_id"
        ),
        "agency_identifier_name": get_agency_name_annotation(tas_id, "agency_id"),
        "submission_period": get_fyp_or_q_notation("submission"),
        "gross_outlay_amount": generate_gross_outlay_amount_derived_field(filters, account_type),
        "gross_outlay_amount_fyb_to_period_end": generate_gross_outlay_amount_derived_field(filters, account_type),
    }

    lmd = "last_modified_date" + NAMING_CONFLICT_DISCRIMINATOR
    if account_type == "award_financial":
        # Separating out last_modified_date like this prevents unnecessary grouping in the full File
        # C TAS download.  Keeping it as MAX caused grouping on every single column in the SQL statement.
        derived_fields[lmd] = Cast("submission__published_date", output_field=DateField())
        derived_fields = award_financial_derivations(derived_fields)
    else:
        derived_fields[lmd] = Cast(Max("submission__published_date"), output_field=DateField())

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id, filters):
    """ Group by federal account (and budget function/subfunction) and SUM all other fields """
    derived_fields = {
        "reporting_agency_name": StringAgg("submission__reporting_agency_name", "; ", distinct=True),
        "budget_function": StringAgg(f"{tas_id}__budget_function_title", "; ", distinct=True),
        "budget_subfunction": StringAgg(f"{tas_id}__budget_subfunction_title", "; ", distinct=True),
        "submission_period": get_fyp_or_q_notation("submission"),
        "agency_identifier_name": get_agency_name_annotation(tas_id, "agency_id"),
        "last_modified_date"
        + NAMING_CONFLICT_DISCRIMINATOR: Cast(Max("submission__published_date"), output_field=DateField()),
        "gross_outlay_amount": Sum(generate_gross_outlay_amount_derived_field(filters, account_type)),
        "gross_outlay_amount_fyb_to_period_end": Sum(generate_gross_outlay_amount_derived_field(filters, account_type)),
    }

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
    )
    derived_fields["award_type"] = Coalesce(
        "award__latest_transaction__contract_data__contract_award_type_desc",
        "award__latest_transaction__assistance_data__assistance_type_desc",
    )
    derived_fields["awarding_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_agency_code",
        "award__latest_transaction__assistance_data__awarding_agency_code",
    )
    derived_fields["awarding_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_agency_name",
        "award__latest_transaction__assistance_data__awarding_agency_name",
    )
    derived_fields["awarding_subagency_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_sub_tier_agency_c",
        "award__latest_transaction__assistance_data__awarding_sub_tier_agency_c",
    )
    derived_fields["awarding_subagency_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_sub_tier_agency_n",
        "award__latest_transaction__assistance_data__awarding_sub_tier_agency_n",
    )
    derived_fields["awarding_office_code"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_office_code",
        "award__latest_transaction__assistance_data__awarding_office_code",
    )
    derived_fields["awarding_office_name"] = Coalesce(
        "award__latest_transaction__contract_data__awarding_office_name",
        "award__latest_transaction__assistance_data__awarding_office_name",
    )
    derived_fields["funding_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_agency_code",
        "award__latest_transaction__assistance_data__funding_agency_code",
    )
    derived_fields["funding_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_agency_name",
        "award__latest_transaction__assistance_data__funding_agency_name",
    )
    derived_fields["funding_sub_agency_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_sub_tier_agency_co",
        "award__latest_transaction__assistance_data__funding_sub_tier_agency_co",
    )
    derived_fields["funding_sub_agency_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_sub_tier_agency_na",
        "award__latest_transaction__assistance_data__funding_sub_tier_agency_na",
    )
    derived_fields["funding_office_code"] = Coalesce(
        "award__latest_transaction__contract_data__funding_office_code",
        "award__latest_transaction__assistance_data__funding_office_code",
    )
    derived_fields["funding_office_name"] = Coalesce(
        "award__latest_transaction__contract_data__funding_office_name",
        "award__latest_transaction__assistance_data__funding_office_name",
    )
    derived_fields["recipient_duns"] = Coalesce(
        "award__latest_transaction__contract_data__awardee_or_recipient_uniqu",
        "award__latest_transaction__assistance_data__awardee_or_recipient_uniqu",
    )
    derived_fields["recipient_name"] = Coalesce(
        "award__latest_transaction__contract_data__awardee_or_recipient_legal",
        "award__latest_transaction__assistance_data__awardee_or_recipient_legal",
    )
    derived_fields["recipient_parent_duns"] = Coalesce(
        "award__latest_transaction__contract_data__ultimate_parent_unique_ide",
        "award__latest_transaction__assistance_data__ultimate_parent_unique_ide",
    )
    derived_fields["recipient_parent_name"] = Coalesce(
        "award__latest_transaction__contract_data__ultimate_parent_legal_enti",
        "award__latest_transaction__assistance_data__ultimate_parent_legal_enti",
    )
    derived_fields["recipient_country"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_country_code",
        "award__latest_transaction__assistance_data__legal_entity_country_code",
    )
    derived_fields["recipient_state"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_state_code",
        "award__latest_transaction__assistance_data__legal_entity_state_code",
    )
    derived_fields["recipient_county"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_county_name",
        "award__latest_transaction__assistance_data__legal_entity_county_name",
    )
    derived_fields["recipient_city"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_city_name",
        "award__latest_transaction__assistance_data__legal_entity_city_name",
    )
    derived_fields["recipient_congressional_district"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_congressional",
        "award__latest_transaction__assistance_data__legal_entity_congressional",
    )
    derived_fields["recipient_zip_code"] = Coalesce(
        "award__latest_transaction__contract_data__legal_entity_zip4",
        Concat(
            "award__latest_transaction__assistance_data__legal_entity_zip5",
            "award__latest_transaction__assistance_data__legal_entity_zip_last4",
        ),
    )
    derived_fields["primary_place_of_performance_country"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perf_country_desc",
        "award__latest_transaction__assistance_data__place_of_perform_country_n",
    )
    derived_fields["primary_place_of_performance_state"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perfor_state_desc",
        "award__latest_transaction__assistance_data__place_of_perform_state_nam",
    )
    derived_fields["primary_place_of_performance_county"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_perform_county_na",
        "award__latest_transaction__assistance_data__place_of_perform_county_na",
    )
    derived_fields["primary_place_of_performance_congressional_district"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_performance_congr",
        "award__latest_transaction__assistance_data__place_of_performance_congr",
    )
    derived_fields["primary_place_of_performance_zip_code"] = Coalesce(
        "award__latest_transaction__contract_data__place_of_performance_zip4a",
        "award__latest_transaction__assistance_data__place_of_performance_zip4a",
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
        output_field=CharField(),
    )

    return derived_fields
