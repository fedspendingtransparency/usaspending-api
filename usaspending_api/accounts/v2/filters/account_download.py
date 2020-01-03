import datetime

from django.db.models import Case, CharField, Max, OuterRef, Subquery, Sum, Value, When, Func, F
from django.db.models.functions import Concat, Coalesce

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import FiscalYearAndQuarter
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.references.models import CGAC, ToptierAgency
from usaspending_api.settings import HOST

AWARD_URL = f"{HOST}/#/award/" if "localhost" in HOST else f"https://{HOST}/#/award/"

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
            # Agency is FREC if the toptier_code is 4 digits, CGAC otherwise
            agency_filter_type = "fr_entity_code" if len(agency.toptier_code) == 4 else "agency_id"
            query_filters["{}__{}".format(tas_id, agency_filter_type)] = agency.toptier_code
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
                "object_class__direct_reimbursable",
            ],
        }
        distinct_cols = unique_columns_mapping[account_type]
        order_by_cols = distinct_cols + ["-reporting_period_start", "-pk"]
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


def get_agency_name_annotation(relation_name: str, cgac_column_name: str) -> Subquery:
    """
    Accepts the Django foreign key relation name for the outer queryset to TreasuryAppropriationAccount
    or FederalAccount join and the CGAC column name and returns an annotation ready Subquery object that
    retrieves the CGAC agency name.
    """
    outer_ref = "{}__{}".format(relation_name, cgac_column_name)
    return Subquery(CGAC.objects.filter(cgac_code=OuterRef(outer_ref)).values("agency_name"))


def generate_treasury_account_query(queryset, account_type, tas_id):
    """ Derive necessary fields for a treasury account-grouped query """
    # Derive treasury_account_symbol, allocation_transfer_agency_name, agency_name, and federal_account_symbol
    # for all account types
    derived_fields = {
        "last_reported_submission_period": FiscalYearAndQuarter("reporting_period_end"),
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
        "allocation_transfer_agency_name": get_agency_name_annotation(tas_id, "allocation_transfer_agency_id"),
        "agency_name": get_agency_name_annotation(tas_id, "agency_id"),
        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        "federal_account_symbol": Concat(
            "{}__federal_account__agency_identifier".format(tas_id),
            Value("-"),
            "{}__federal_account__main_account_code".format(tas_id),
        ),
        "submission_period": FiscalYearAndQuarter("reporting_period_end"),
    }

    # Derive recipient_parent_name
    if account_type == "award_financial":
        derived_fields = award_financial_derivations(derived_fields)

    return queryset.annotate(**derived_fields)


def generate_federal_account_query(queryset, account_type, tas_id):
    """ Group by federal account (and budget function/subfunction) and SUM all other fields """
    derived_fields = {
        "last_reported_submission_period": Max(FiscalYearAndQuarter("reporting_period_end")),
        # federal_account_symbol: fed_acct_AID-fed_acct_MAC
        "federal_account_symbol": Concat(
            "{}__federal_account__agency_identifier".format(tas_id),
            Value("-"),
            "{}__federal_account__main_account_code".format(tas_id),
        ),
        "agency_name": get_agency_name_annotation(tas_id, "agency_id"),
        "submission_period": FiscalYearAndQuarter("reporting_period_end"),
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

        # For all files, filter up to and including the FYQ
        reporting_period_start = "reporting_period_start__gte"
        reporting_period_end = "reporting_period_end__lte"
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
        "award__latest_transaction__contract_data__legal_entity_zip5",
        "award__latest_transaction__assistance_data__legal_entity_zip5",
    )
    derived_fields["usaspending_permalink"] = Case(
        When(
            **{
                "award__generated_unique_award_id__isnull": False,
                "then": Concat(
                    Value(AWARD_URL), Func(F("award__generated_unique_award_id"), function="urlencode"), Value("/")
                ),
            }
        ),
        default=Value(""),
        output_field=CharField(),
    )

    return derived_fields
