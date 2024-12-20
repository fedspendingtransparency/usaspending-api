import copy
import logging

from collections import OrderedDict
from decimal import Decimal
from django.db.models import Sum, F, Subquery
from django.utils.text import slugify
from typing import Optional


from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    ParentAward,
    TransactionFABS,
    TransactionFPDS,
    TransactionNormalized,
)
from usaspending_api.awards.v2.data_layer.orm_mappers import (
    FABS_ASSISTANCE_FIELDS,
    FABS_AWARD_FIELDS,
    FPDS_AWARD_FIELDS,
    FPDS_CONTRACT_FIELDS,
)
from usaspending_api.awards.v2.data_layer.orm_utils import delete_keys_from_dict, split_mapper_into_qs
from usaspending_api.common.helpers.business_categories_helper import get_business_category_display_names
from usaspending_api.common.helpers.data_constants import state_code_from_name, state_name_from_code
from usaspending_api.common.helpers.date_helper import get_date_from_datetime
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.recipient_lookups import obtain_recipient_uri
from usaspending_api.references.models import (
    Agency,
    Cfda,
    DisasterEmergencyFundCode,
    NAICS,
    PSC,
    SubtierAgency,
    ToptierAgencyPublishedDABSView,
)
from usaspending_api.awards.v2.data_layer.sql import defc_sql
from usaspending_api.search.models import AwardSearch

logger = logging.getLogger("console")


def construct_assistance_response(requested_award_dict: dict) -> OrderedDict:
    """Build an Assistance Award summary object to send as an API response"""

    response = OrderedDict()
    award = fetch_award_details(requested_award_dict, FABS_AWARD_FIELDS)
    if not award:
        return None

    response.update(award)

    account_data = fetch_account_details_award(award["id"])
    response.update(account_data)
    transaction = fetch_fabs_details_by_pk(award["_trx"], FABS_ASSISTANCE_FIELDS)

    response["record_type"] = transaction["record_type"]
    response["cfda_info"] = fetch_all_cfda_details(award)
    response["transaction_obligated_amount"] = fetch_transaction_obligated_amount_by_internal_award_id(award["id"])
    response["funding_agency"] = fetch_agency_details(response["_funding_agency_id"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency_id"])
    if response["awarding_agency"]:
        response["awarding_agency"]["office_agency_name"] = transaction["_awarding_office_name"]
    response["period_of_performance"] = OrderedDict(
        [
            ("start_date", award["_start_date"]),
            ("end_date", award["_end_date"]),
            ("last_modified_date", get_date_from_datetime(transaction["_modified_at"])),
        ]
    )
    response["recipient"] = create_recipient_object(transaction)
    response["executive_details"] = create_officers_object(award)
    response["place_of_performance"] = create_place_of_performance_object(transaction)
    response["total_outlay"] = fetch_total_outlays(award["id"])
    response["funding_opportunity"] = {
        "number": transaction["_funding_opportunity_number"],
        "goals": transaction["_funding_opportunity_goals"],
    }
    return delete_keys_from_dict(response)


def construct_contract_response(requested_award_dict: dict) -> OrderedDict:
    """Build a Procurement Award summary object to send as an API response"""

    response = OrderedDict()
    award = fetch_award_details(requested_award_dict, FPDS_AWARD_FIELDS)
    if not award:
        return None

    response.update(award)

    account_data = fetch_account_details_award(award["id"])
    response.update(account_data)

    transaction = fetch_fpds_details_by_pk(award["_trx"], FPDS_CONTRACT_FIELDS)

    response["parent_award"] = fetch_contract_parent_award_details(
        award["_parent_award_piid"], award["_fpds_parent_agency_id"]
    )
    response["latest_transaction_contract_data"] = transaction
    response["funding_agency"] = fetch_agency_details(response["_funding_agency_id"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency_id"])
    if response["awarding_agency"]:
        response["awarding_agency"]["office_agency_name"] = transaction["_awarding_office_name"]
    response["period_of_performance"] = OrderedDict(
        [
            ("start_date", award["_start_date"]),
            ("end_date", award["_end_date"]),
            ("last_modified_date", transaction["_last_modified"]),
            ("potential_end_date", transaction["_period_of_perf_potential_e"]),
        ]
    )
    response["recipient"] = create_recipient_object(transaction)
    response["executive_details"] = create_officers_object(award)
    response["place_of_performance"] = create_place_of_performance_object(transaction)
    if transaction["product_or_service_code"]:
        response["psc_hierarchy"] = fetch_psc_hierarchy(transaction["product_or_service_code"])
    if transaction["naics"]:
        response["naics_hierarchy"] = fetch_naics_hierarchy(transaction["naics"])
    response["total_outlay"] = fetch_total_outlays(award["id"])
    return delete_keys_from_dict(response)


def construct_idv_response(requested_award_dict: dict) -> OrderedDict:
    """Build a Procurement IDV summary object to send as an API response"""

    idv_specific_award_fields = OrderedDict(
        [
            ("period_of_performance_star", "_start_date"),
            ("last_modified", "_last_modified_date"),
            ("ordering_period_end_date", "_end_date"),
        ]
    )

    mapper = copy.deepcopy(FPDS_CONTRACT_FIELDS)
    mapper.update(idv_specific_award_fields)

    response = OrderedDict()
    award = fetch_award_details(requested_award_dict, FPDS_AWARD_FIELDS)
    if not award:
        return None
    response.update(award)

    account_data = fetch_account_details_award(award["id"])
    response.update(account_data)

    transaction = fetch_fpds_details_by_pk(award["_trx"], mapper)

    response["parent_award"] = fetch_idv_parent_award_details(award["generated_unique_award_id"])
    response["latest_transaction_contract_data"] = transaction
    response["funding_agency"] = fetch_agency_details(response["_funding_agency_id"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency_id"])
    if response["awarding_agency"]:
        response["awarding_agency"]["office_agency_name"] = transaction["_awarding_office_name"]
    response["period_of_performance"] = OrderedDict(
        [
            ("start_date", award["_start_date"]),
            ("end_date", transaction["_end_date"]),
            ("last_modified_date", transaction["_last_modified_date"]),
            ("potential_end_date", transaction["_period_of_perf_potential_e"]),
        ]
    )
    response["recipient"] = create_recipient_object(transaction)
    response["executive_details"] = create_officers_object(award)
    response["place_of_performance"] = create_place_of_performance_object(transaction)
    if transaction["product_or_service_code"]:
        response["psc_hierarchy"] = fetch_psc_hierarchy(transaction["product_or_service_code"])
    if transaction["naics"]:
        response["naics_hierarchy"] = fetch_naics_hierarchy(transaction["naics"])
    response["total_outlay"] = fetch_total_outlays(award["id"])
    return delete_keys_from_dict(response)


def create_recipient_object(db_row_dict: dict) -> OrderedDict:
    return OrderedDict(
        [
            (
                "recipient_hash",
                obtain_recipient_uri(
                    db_row_dict["_recipient_name"],
                    db_row_dict["_recipient_uei"],
                    db_row_dict["_parent_recipient_uei"],
                    db_row_dict["_recipient_unique_id"],
                    db_row_dict["_parent_recipient_unique_id"],
                ),
            ),
            ("recipient_name", db_row_dict["_recipient_name"]),
            ("recipient_uei", db_row_dict["_recipient_uei"]),
            ("recipient_unique_id", db_row_dict["_recipient_unique_id"]),
            (
                "parent_recipient_hash",
                obtain_recipient_uri(
                    db_row_dict["_parent_recipient_name"],
                    db_row_dict["_parent_recipient_uei"],
                    None,  # parent_recipient_uei
                    db_row_dict["_parent_recipient_unique_id"],
                    None,  # parent_recipient_unique_id
                    True,  # is_parent_recipient
                ),
            ),
            ("parent_recipient_name", db_row_dict["_parent_recipient_name"]),
            ("parent_recipient_uei", db_row_dict["_parent_recipient_uei"]),
            ("parent_recipient_unique_id", db_row_dict["_parent_recipient_unique_id"]),
            (
                "business_categories",
                get_business_category_display_names(
                    fetch_business_categories_by_transaction_id(db_row_dict["_transaction_id"])
                ),
            ),
            (
                "location",
                OrderedDict(
                    [
                        ("location_country_code", db_row_dict["_rl_location_country_code"]),
                        ("country_name", db_row_dict["_rl_country_name"]),
                        ("state_code", db_row_dict["_rl_state_code"]),
                        ("state_name", db_row_dict["_rl_state_name"]),
                        ("city_name", db_row_dict["_rl_city_name"] or db_row_dict.get("_rl_foreign_city")),
                        ("county_code", db_row_dict["_rl_county_code"]),
                        ("county_name", db_row_dict["_rl_county_name"]),
                        ("address_line1", db_row_dict["_rl_address_line1"]),
                        ("address_line2", db_row_dict["_rl_address_line2"]),
                        ("address_line3", db_row_dict["_rl_address_line3"]),
                        ("congressional_code", db_row_dict["_rl_congressional_code_current"]),
                        ("zip4", db_row_dict.get("_rl_zip_last_4") or db_row_dict.get("_rl_zip4")),
                        ("zip5", db_row_dict["_rl_zip5"]),
                        ("foreign_postal_code", db_row_dict.get("_rl_foreign_postal_code")),
                        ("foreign_province", db_row_dict.get("_rl_foreign_province")),
                    ]
                ),
            ),
        ]
    )


def create_place_of_performance_object(db_row_dict: dict) -> OrderedDict:
    return OrderedDict(
        [
            ("location_country_code", db_row_dict["_pop_location_country_code"]),
            ("country_name", db_row_dict["_pop_country_name"]),
            ("county_code", db_row_dict["_pop_county_code"]),
            ("county_name", db_row_dict["_pop_county_name"]),
            ("city_name", db_row_dict["_pop_city_name"]),
            (
                "state_code",
                (
                    db_row_dict["_pop_state_code"]
                    if db_row_dict["_pop_state_code"]
                    else state_code_from_name(db_row_dict["_pop_state_name"])
                ),
            ),
            (
                "state_name",
                (
                    db_row_dict["_pop_state_name"]
                    if db_row_dict["_pop_state_name"]
                    else state_name_from_code(db_row_dict["_pop_state_code"])
                ),
            ),
            ("congressional_code", db_row_dict["_pop_congressional_code_current"]),
            ("zip4", db_row_dict["_pop_zip4"]),
            ("zip5", db_row_dict["_pop_zip5"]),
            ("address_line1", None),
            ("address_line2", None),
            ("address_line3", None),
            ("foreign_province", db_row_dict.get("_pop_foreign_province")),
            ("foreign_postal_code", None),
        ]
    )


def create_officers_object(award: dict) -> dict:
    """Construct the Executive Compensation Object"""
    return {
        "officers": [
            {
                "name": award.get("_officer_{}_name".format(officer_num)),
                "amount": award.get("_officer_{}_amount".format(officer_num)),
            }
            for officer_num in range(1, 6)
        ]
    }


def fetch_award_details(filter_q: dict, mapper_fields: OrderedDict) -> dict:
    vals, ann = split_mapper_into_qs(mapper_fields)
    return Award.objects.filter(**filter_q).values(*vals).annotate(**ann).first()


def fetch_contract_parent_award_details(parent_piid: str, parent_fpds_agency: str) -> Optional[OrderedDict]:
    parent_guai = "CONT_IDV_{}_{}".format(parent_piid or "NONE", parent_fpds_agency or "NONE")

    parent_award_ids = (
        ParentAward.objects.filter(generated_unique_award_id=parent_guai)
        .annotate(parent_award_award_id=F("award_id"), parent_award_guai=F("generated_unique_award_id"))
        .values("parent_award_award_id", "parent_award_guai")
        .first()
    )

    return _fetch_parent_award_details(parent_award_ids)


def fetch_idv_parent_award_details(guai: str) -> Optional[OrderedDict]:
    parent_award_ids = (
        ParentAward.objects.filter(generated_unique_award_id=guai, parent_award__isnull=False)
        .annotate(
            parent_award_award_id=F("parent_award__award_id"),
            parent_award_guai=F("parent_award__generated_unique_award_id"),
        )
        .values("parent_award_award_id", "parent_award_guai")
        .first()
    )

    return _fetch_parent_award_details(parent_award_ids)


def _fetch_parent_award_details(parent_award_ids: dict) -> Optional[OrderedDict]:
    if not parent_award_ids:
        return None

    # These are not exact query paths but instead expected aliases to allow reuse
    parent_award_award_id = parent_award_ids["parent_award_award_id"]
    parent_award_guai = parent_award_ids["parent_award_guai"]

    parent_award = (
        AwardSearch.objects.filter(award_id=parent_award_award_id)
        .values(
            "latest_transaction__contract_data__agency_id",
            "latest_transaction__contract_data__idv_type_description",
            "latest_transaction__contract_data__multiple_or_single_aw_desc",
            "latest_transaction__contract_data__piid",
            "latest_transaction__contract_data__type_of_idc_description",
        )
        .first()
    )

    if not parent_award:
        logging.debug("Unable to find award for award id %s" % parent_award_award_id)
        return None

    parent_sub_agency = (
        SubtierAgency.objects.filter(subtier_code=parent_award["latest_transaction__contract_data__agency_id"])
        .values("name", "subtier_agency_id")
        .first()
    )
    parent_agency = (
        (
            Agency.objects.filter(
                toptier_flag=True,
                toptier_agency_id=Subquery(
                    Agency.objects.filter(
                        subtier_agency_id__isnull=False, subtier_agency_id=parent_sub_agency["subtier_agency_id"]
                    ).values("toptier_agency_id")
                ),
            )
            .values("id", "toptier_agency_id", "toptier_agency__name")
            .first()
        )
        if parent_sub_agency
        else None
    )

    toptier_agency_id = parent_agency["toptier_agency_id"] if parent_agency else None
    agency_name = parent_agency["toptier_agency__name"] if parent_agency else None

    has_agency_page = agency_has_file_c_submission(toptier_agency_id)

    parent_object = OrderedDict(
        [
            ("agency_id", parent_agency["id"] if parent_agency else None),
            ("agency_name", agency_name),
            ("agency_slug", slugify(agency_name) if has_agency_page else None),
            ("sub_agency_id", parent_award["latest_transaction__contract_data__agency_id"]),
            ("sub_agency_name", parent_sub_agency["name"] if parent_sub_agency else None),
            ("award_id", parent_award_award_id),
            ("generated_unique_award_id", parent_award_guai),
            ("idv_type_description", parent_award["latest_transaction__contract_data__idv_type_description"]),
            (
                "multiple_or_single_aw_desc",
                parent_award["latest_transaction__contract_data__multiple_or_single_aw_desc"],
            ),
            ("piid", parent_award["latest_transaction__contract_data__piid"]),
            ("type_of_idc_description", parent_award["latest_transaction__contract_data__type_of_idc_description"]),
        ]
    )

    return parent_object


def fetch_fabs_details_by_pk(primary_key: int, mapper: OrderedDict) -> dict:
    vals, ann = split_mapper_into_qs(mapper)
    return TransactionFABS.objects.filter(pk=primary_key).values(*vals).annotate(**ann).first()


def fetch_fpds_details_by_pk(primary_key: int, mapper: OrderedDict) -> dict:
    vals, ann = split_mapper_into_qs(mapper)
    return TransactionFPDS.objects.filter(pk=primary_key).values(*vals).annotate(**ann).first()


def fetch_latest_ec_details(award_id: int, mapper: OrderedDict, transaction_type: str) -> dict:
    vals, ann = split_mapper_into_qs(mapper)
    model = TransactionFPDS if transaction_type == "fpds" else TransactionFABS
    retval = (
        model.objects.filter(transaction__award_id=award_id, officer_1_name__isnull=False)
        .values(*vals)
        .annotate(**ann)
        .order_by("-action_date")
    )
    return retval.first()


def agency_has_file_c_submission(toptier_agency_id):
    return ToptierAgencyPublishedDABSView.objects.filter(toptier_agency_id=toptier_agency_id).exists()


def fetch_agency_details(agency_id: int) -> Optional[dict]:
    values = [
        "toptier_agency_id",
        "toptier_agency__toptier_code",
        "toptier_agency__name",
        "toptier_agency__abbreviation",
        "subtier_agency__subtier_code",
        "subtier_agency__name",
        "subtier_agency__abbreviation",
    ]
    agency = Agency.objects.filter(pk=agency_id).values(*values).first()

    agency_details = None
    if agency:
        has_agency_page = agency_has_file_c_submission(agency["toptier_agency_id"])
        agency_details = {
            "id": agency_id,
            "has_agency_page": has_agency_page,
            "toptier_agency": {
                "name": agency["toptier_agency__name"],
                "code": agency["toptier_agency__toptier_code"],
                "abbreviation": agency["toptier_agency__abbreviation"],
                "slug": slugify(agency["toptier_agency__name"]) if has_agency_page else None,
            },
            "subtier_agency": {
                "name": agency["subtier_agency__name"],
                "code": agency["subtier_agency__subtier_code"],
                "abbreviation": agency["subtier_agency__abbreviation"],
            },
        }
    return agency_details


def fetch_business_categories_by_transaction_id(transaction_id: int) -> list:
    tn = TransactionNormalized.objects.filter(pk=transaction_id).values("business_categories").first()

    if tn:
        return tn["business_categories"]
    return []


def normalize_cfda_number_format(fabs_transaction: dict) -> str:
    """Normalize a CFDA number to 6 digits by padding 0 in case the value was truncated"""
    cfda_number = fabs_transaction.get("cfda_number")
    if cfda_number and len(cfda_number) < 6:
        cfda_number += "0" * (6 - len(cfda_number))

    return cfda_number


def fetch_all_cfda_details(award: dict) -> list:
    fabs_values = ["cfda_number", "federal_action_obligation", "non_federal_funding_amount", "total_funding_amount"]
    queryset = TransactionFABS.objects.filter(transaction__award_id=award["id"]).values(*fabs_values)
    cfda_dicts = {}
    for transaction in queryset:
        clean_cfda_number_str = normalize_cfda_number_format(transaction)
        if cfda_dicts.get(clean_cfda_number_str):
            cfda_dicts.update(
                {
                    clean_cfda_number_str: {
                        "federal_action_obligation": cfda_dicts[clean_cfda_number_str]["federal_action_obligation"]
                        + Decimal(transaction["federal_action_obligation"] or 0),
                        "non_federal_funding_amount": cfda_dicts[clean_cfda_number_str]["non_federal_funding_amount"]
                        + Decimal(transaction["non_federal_funding_amount"] or 0),
                        "total_funding_amount": cfda_dicts[clean_cfda_number_str]["total_funding_amount"]
                        + Decimal(transaction["total_funding_amount"] or 0),
                    }
                }
            )
        else:
            cfda_dicts.update(
                {
                    clean_cfda_number_str: {
                        "federal_action_obligation": Decimal(transaction["federal_action_obligation"] or 0),
                        "non_federal_funding_amount": Decimal(transaction["non_federal_funding_amount"] or 0),
                        "total_funding_amount": Decimal(transaction["total_funding_amount"] or 0),
                    }
                }
            )

    final_cfda_objects = []
    for cfda_number in cfda_dicts.keys():
        details = fetch_cfda_details_using_cfda_number(cfda_number)
        if details.get("url") == "None;":
            details.update({"url": None})
        final_cfda_objects.append(
            OrderedDict(
                [
                    ("applicant_eligibility", details.get("applicant_eligibility")),
                    ("beneficiary_eligibility", details.get("beneficiary_eligibility")),
                    ("cfda_federal_agency", details.get("federal_agency")),
                    ("cfda_number", cfda_number),
                    ("cfda_objectives", details.get("objectives")),
                    ("cfda_obligations", details.get("obligations")),
                    ("cfda_popular_name", details.get("popular_name")),
                    ("cfda_title", details.get("program_title")),
                    ("cfda_website", details.get("website_address")),
                    ("federal_action_obligation_amount", cfda_dicts[cfda_number]["federal_action_obligation"]),
                    ("non_federal_funding_amount", cfda_dicts[cfda_number]["non_federal_funding_amount"]),
                    ("sam_website", details.get("url")),
                    ("total_funding_amount", cfda_dicts[cfda_number]["total_funding_amount"]),
                ]
            )
        )
    final_cfda_objects.sort(key=lambda cfda: cfda["total_funding_amount"], reverse=True)
    return final_cfda_objects


def fetch_cfda_details_using_cfda_number(cfda: str) -> dict:
    values = [
        "applicant_eligibility",
        "beneficiary_eligibility",
        "program_title",
        "objectives",
        "federal_agency",
        "website_address",
        "url",
        "obligations",
        "popular_name",
    ]
    cfda_details = Cfda.objects.filter(program_number=cfda).values(*values).first()

    return cfda_details or {}


def fetch_transaction_obligated_amount_by_internal_award_id(internal_award_id: int) -> Optional[Decimal]:
    _sum = FinancialAccountsByAwards.objects.filter(award_id=internal_award_id).aggregate(
        Sum("transaction_obligated_amount")
    )
    if _sum:
        return _sum["transaction_obligated_amount__sum"]

    return None


def fetch_psc_hierarchy(psc_code: str) -> dict:
    codes = [psc_code, psc_code[:2], psc_code[:1], psc_code[:3] if psc_code[0] == "A" else None]
    toptier_code = {}
    midtier_code = {}
    subtier_code = {}  # only used for R&D codes which start with "A"
    base_code = {}
    if psc_code[0].isalpha():  # we only want to look for the toptier code for services, which start with letters
        try:
            psc_top = PSC.objects.get(code=codes[2])
            toptier_code = {"code": psc_top.code, "description": psc_top.description}
        except PSC.DoesNotExist:
            pass
    try:
        psc_mid = PSC.objects.get(code=codes[1])
        midtier_code = {"code": psc_mid.code, "description": psc_mid.description}
    except PSC.DoesNotExist:
        pass
    try:
        psc = PSC.objects.get(code=codes[0])
        base_code = {"code": psc.code, "description": psc.description}
    except PSC.DoesNotExist:
        pass
    if codes[3] is not None:  # don't bother looking for 3 digit codes unless they start with "A"
        try:
            psc_rd = PSC.objects.get(code=codes[3])
            subtier_code = {"code": psc_rd.code, "description": psc_rd.description}
        except PSC.DoesNotExist:
            pass

    results = {
        "toptier_code": toptier_code,
        "midtier_code": midtier_code,
        "subtier_code": subtier_code,
        "base_code": base_code,
    }
    return results


def fetch_naics_hierarchy(naics: str) -> dict:
    codes = [naics, naics[:4], naics[:2]]
    toptier_code = {}
    midtier_code = {}
    base_code = {}
    try:
        toptier = NAICS.objects.get(code=codes[2])
        toptier_code = {"code": toptier.code, "description": toptier.description}
    except NAICS.DoesNotExist:
        pass
    try:
        midtier = NAICS.objects.get(code=codes[1])
        midtier_code = {"code": midtier.code, "description": midtier.description}
    except NAICS.DoesNotExist:
        pass
    try:
        base = NAICS.objects.get(code=codes[0])
        base_code = {"code": base.code, "description": base.description}
    except NAICS.DoesNotExist:
        pass
    results = {"toptier_code": toptier_code, "midtier_code": midtier_code, "base_code": base_code}
    return results


def fetch_account_details_award(award_id: int) -> dict:
    award_id_sql = "faba.award_id = {award_id}".format(award_id=award_id)
    results = execute_sql_to_ordered_dictionary(defc_sql.format(award_id_sql=award_id_sql))
    outlay_by_code = []
    obligation_by_code = []
    total_outlay = 0
    total_obligations = 0
    defcs = DisasterEmergencyFundCode.objects.all().values_list("code", flat=True)
    for row in results:
        if row["disaster_emergency_fund_code"] in defcs:
            total_outlay += row["total_outlay"]
            total_obligations += row["obligated_amount"]
        outlay_by_code.append({"code": row["disaster_emergency_fund_code"], "amount": row["total_outlay"]})
        obligation_by_code.append({"code": row["disaster_emergency_fund_code"], "amount": row["obligated_amount"]})
    results = {
        "total_account_outlay": total_outlay,
        "total_account_obligation": total_obligations,
        "account_outlays_by_defc": outlay_by_code,
        "account_obligations_by_defc": obligation_by_code,
    }
    return results


def fetch_total_outlays(award_id: int) -> dict:
    sql = """
    with date_signed_outlay_amounts (award_id, last_period_total_outlay) as (
        SELECT
            faba.award_id,
            COALESCE(
                sum(COALESCE(faba.gross_outlay_amount_by_award_cpe,0)
                + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)), 0)
            as last_period_total_outlay
        FROM
            financial_accounts_by_awards faba
        INNER JOIN submission_attributes sa
            ON faba.submission_id = sa.submission_id
        WHERE
            {award_id_sql}
            AND
            sa.is_final_balances_for_fy = TRUE
        GROUP BY faba.award_id
    )
    SELECT sum(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN (
        COALESCE(faba.gross_outlay_amount_by_award_cpe,0)
        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
    ) END) AS total_outlay
    FROM
        financial_accounts_by_awards faba
    INNER JOIN submission_attributes sa
        ON faba.submission_id = sa.submission_id
    INNER JOIN date_signed_outlay_amounts o ON faba.award_id = o.award_id
    WHERE
        o.last_period_total_outlay != 0
        AND
        (
            faba.gross_outlay_amount_by_award_cpe IS NOT NULL
            AND
            faba.gross_outlay_amount_by_award_cpe != 0
        );
    """
    results = execute_sql_to_ordered_dictionary(sql.format(award_id_sql=f"faba.award_id = {award_id}"))
    if len(results) > 0:
        return results[0]["total_outlay"]
    return None
