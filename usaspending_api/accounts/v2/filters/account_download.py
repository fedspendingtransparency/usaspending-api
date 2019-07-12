import datetime

from django.db.models import Case, CharField, OuterRef, Subquery, Sum, Value, When
from django.db.models.functions import Concat, Coalesce

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import ToptierAgency

"""
Account Download Logic

Account Balances (A file):
    - Treasury Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
        2. Only include the most recently submitted TASs (uniqueness based on TAS)
    - Federal Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
        2. Only include the most recently submitted TASs (uniqueness based on TAS)
        3. Group by Federal Accounts
Account Breakdown by Program Activity & Object Class (B file):
    - Treasury Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
        2. Only include the most recently submitted TASs (uniqueness based on TAS/PA/OC/DR)
    - Federal Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
        2. Only include the most recently submitted TASs (uniqueness based on TAS/PA/OC/DR)
        3. Group by Federal Accounts
Account Breakdown by Award (C file):
    - Treasury Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
    - Federal Account
        1. Get all TASs matching the filers from Q1 to the FSQ selected
        2. Group by Federal Accounts
"""


def account_download_filter(account_type, download_table, filters, account_level="treasury_account"):
    query_filters = {}
    tas_id = "treasury_account_identifier" if account_type == "account_balances" else "treasury_account"

    # Filter by Agency, if provided
    if filters.get("agency", False) and filters["agency"] != "all":
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters["agency"]).first()
        if agency:
            # Agency is FREC if the cgac_code is 4 digits, CGAC otherwise
            agency_filter_type = "fr_entity_code" if len(agency.cgac_code) == 4 else "agency_id"
            query_filters["{}__{}".format(tas_id, agency_filter_type)] = agency.cgac_code
        else:
            raise InvalidParameterException("Agency with that ID does not exist")

    # Filter by Federal Account, if provided
    if filters.get("federal_account", False) and filters["federal_account"] != "all":
        federal_account_obj = FederalAccount.objects.filter(id=filters["federal_account"]).first()
        if federal_account_obj:
            query_filters["{}__federal_account__id".format(tas_id)] = filters["federal_account"]
        else:
            raise InvalidParameterException("Federal Account with that ID does not exist")

    # Filter by Budget Function, if provided
    if filters.get("budget_function", False) and filters["budget_function"] != "all":
        query_filters["{}__budget_function_code".format(tas_id)] = filters["budget_function"]

    # Filter by Budget SubFunction, if provided
    if filters.get("budget_subfunction", False) and filters["budget_subfunction"] != "all":
        query_filters["{}__budget_subfunction_code".format(tas_id)] = filters["budget_subfunction"]

    # Filter by Fiscal Year and Quarter
    reporting_period_start, reporting_period_end, start_date, end_date = retrieve_fyq_filters(
        account_type, account_level, filters
    )
    query_filters[reporting_period_start] = start_date
    query_filters[reporting_period_end] = end_date

    # Create the base queryset
    queryset = download_table.objects

    if account_type in ["account_balances", "object_class_program_activity"]:
        # only include the latest TASs, not all of them
        unique_id_mapping = {
            "account_balances": "appropriation_account_balances_id",
            "object_class_program_activity": "financial_accounts_by_program_activity_object_class_id",
        }
        unique_columns_mapping = {
            "account_balances": ["treasury_account_identifier__tas_rendering_label"],
            "object_class_program_activity": [
                "treasury_account__tas_rendering_label",
                "program_activity__program_activity_code",
                "object_class__object_class",
            ],
        }
        distinct_cols = unique_columns_mapping[account_type]
        order_by_cols = distinct_cols + ["-reporting_period_start"]
        latest_ids_q = download_table.objects.filter(**query_filters).distinct(*distinct_cols).order_by(*order_by_cols)
        latest_ids = list(latest_ids_q.values_list(unique_id_mapping[account_type], flat=True))
        if latest_ids:
            query_filters["{}__in".format(unique_id_mapping[account_type])] = latest_ids

    # Make derivations based on the account level
    if account_level == "treasury_account":
        queryset = generate_treasury_account_query(queryset, account_type, tas_id)
    elif account_level == "federal_account":
        queryset = generate_federal_account_query(queryset, account_type, tas_id)
    else:
        raise InvalidParameterException(
            'Invalid Parameter: account_level must be either "federal_account" or ' '"treasury_account"'
        )

    # Apply filter and return
    return queryset.filter(**query_filters)


def generate_treasury_account_query(queryset, account_type, tas_id):
    """ Derive necessary fields for a treasury account-grouped query """
    # Derive treasury_account_symbol, allocation_transfer_agency_name, agency_name, and federal_account_symbol
    # for all account types
    ata_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef("{}__allocation_transfer_agency_id".format(tas_id)))
    agency_name_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef("{}__agency_id".format(tas_id)))
    derived_fields = {
        # treasury_account_symbol: [ATA-]AID-BPOA/EPOA-MAC-SAC or [ATA-]AID-"X"-MAC-SAC
        "treasury_account_symbol": Concat(
            Case(
                When(
                    **{
                        "{}__allocation_transfer_agency_id__isnull".format(tas_id): False,
                        "then": Concat("{}__allocation_transfer_agency_id".format(tas_id), Value("-")),
                    }
                ),
                default=Value(""),
                output_field=CharField(),
            ),
            "{}__agency_id".format(tas_id),
            Value("-"),
            Case(
                When(**{"{}__availability_type_code".format(tas_id): "X", "then": Value("X")}),
                default=Concat(
                    "{}__beginning_period_of_availability".format(tas_id),
                    Value("/"),
                    "{}__ending_period_of_availability".format(tas_id),
                ),
                output_field=CharField(),
            ),
            Value("-"),
            "{}__main_account_code".format(tas_id),
            Value("-"),
            "{}__sub_account_code".format(tas_id),
            output_field=CharField(),
        ),
        # allocation_transfer_agency_name: name of the ToptierAgency with CGAC matching allocation_transfer_agency_id
        "allocation_transfer_agency_name": Subquery(ata_subquery.values("name")[:1]),
        # agency_name: name of the ToptierAgency with CGAC matching agency_id
        "agency_name": Subquery(agency_name_subquery.values("name")[:1]),
        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        "federal_account_symbol": Concat(
            "{}__federal_account__agency_identifier".format(tas_id),
            Value("-"),
            "{}__federal_account__main_account_code".format(tas_id),
        ),
    }

    # Derive recipient_parent_name
    if account_type == "award_financial":
        derived_fields = award_financial_derivations(derived_fields)

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id):
    """ Group by federal account (and budget function/subfunction) and SUM all other fields """
    # Derive the federal_account_symbol and agency_name
    agency_name_subquery = ToptierAgency.objects.filter(
        cgac_code=OuterRef("{}__federal_account__agency_identifier".format(tas_id))
    )
    derived_fields = {
        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        "federal_account_symbol": Concat(
            "{}__federal_account__agency_identifier".format(tas_id),
            Value("-"),
            "{}__federal_account__main_account_code".format(tas_id),
        ),
        # agency_name: name of the ToptierAgency with CGAC matching federal_account__agency_identifier
        "agency_name": Subquery(agency_name_subquery.values("name")[:1]),
    }

    # Derive recipient_parent_name for award_financial downloads
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
        "gross_outlay_amount",
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


def retrieve_fyq_filters(account_type, account_level, filters):
    """ Apply a filter by Fiscal Year and Quarter """
    if filters.get("fy", False) and filters.get("quarter", False):
        start_date, end_date = start_and_end_dates_from_fyq(filters["fy"], filters["quarter"])

        reporting_period_start = "reporting_period_start"
        reporting_period_end = "reporting_period_end"

        # For all files, filter up to and including the FYQ
        reporting_period_start = "{}__gte".format(reporting_period_start)
        reporting_period_end = "{}__lte".format(reporting_period_end)
        if str(filters["quarter"]) != "1":
            start_date = datetime.date(filters["fy"] - 1, 10, 1)
    else:
        raise InvalidParameterException("fy and quarter are required parameters")

    return reporting_period_start, reporting_period_end, start_date, end_date


def award_financial_derivations(derived_fields):
    derived_fields["recipient_parent_name"] = Case(
        When(
            award__latest_transaction__type__in=list(contract_type_mapping.keys()),
            then="award__latest_transaction__contract_data__ultimate_parent_legal_enti",
        ),
        default="award__latest_transaction__assistance_data__ultimate_parent_legal_enti",
        output_field=CharField(),
    )
    derived_fields["award_type_code"] = Coalesce(
        "award__latest_transaction__contract_data__contract_award_type",
        "award__latest_transaction__assistance_data__assistance_type",
    )
    derived_fields["award_type"] = Coalesce(
        "award__latest_transaction__contract_data__contract_award_type_desc",
        "award__latest_transaction__assistance_data__assistance_type_desc",
    )
    return derived_fields
