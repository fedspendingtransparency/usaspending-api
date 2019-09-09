import copy
import logging
from decimal import Decimal

from collections import OrderedDict
from django.db.models import Sum

from usaspending_api.awards.v2.data_layer.orm_mappers import (
    FABS_AWARD_FIELDS,
    FPDS_CONTRACT_FIELDS,
    FPDS_AWARD_FIELDS,
    FABS_ASSISTANCE_FIELDS,
)
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    TransactionFABS,
    TransactionFPDS,
    ParentAward,
)
from usaspending_api.awards.v2.data_layer.orm_utils import delete_keys_from_dict, split_mapper_into_qs
from usaspending_api.common.helpers.business_categories_helper import get_business_category_display_names
from usaspending_api.common.helpers.date_helper import get_date_from_datetime
from usaspending_api.common.helpers.data_constants import state_code_from_name, state_name_from_code
from usaspending_api.common.recipient_lookups import obtain_recipient_uri
from usaspending_api.references.models import Agency, LegalEntity, Cfda, SubtierAgency


logger = logging.getLogger("console")


def construct_assistance_response(requested_award_dict):
    """
        Build the Python object to return FABS Award summary or meta-data via the API

        parameter(s): `requested_award` either award.id (int) or generated_unique_award_id (str)
        returns: an OrderedDict
    """

    response = OrderedDict()
    award = fetch_award_details(requested_award_dict, FABS_AWARD_FIELDS)
    if not award:
        return None
    response.update(award)

    response["executive_details"] = create_officers_object(award, FABS_ASSISTANCE_FIELDS, "fabs")

    transaction = fetch_fabs_details_by_pk(award["_trx"], FABS_ASSISTANCE_FIELDS)

    response["cfda_info"] = fetch_all_cfda_details(award)
    response["transaction_obligated_amount"] = fetch_transaction_obligated_amount_by_internal_award_id(award["id"])

    response["funding_agency"] = fetch_agency_details(response["_funding_agency"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency"])
    if response["awarding_agency"]:
        response["awarding_agency"]["office_agency_name"] = transaction["_awarding_office_name"]
    response["period_of_performance"] = OrderedDict(
        [
            ("start_date", award["_start_date"]),
            ("end_date", award["_end_date"]),
            ("last_modified_date", get_date_from_datetime(transaction["_modified_at"])),
        ]
    )
    transaction["_lei"] = award["_lei"]
    response["recipient"] = create_recipient_object(transaction)
    response["place_of_performance"] = create_place_of_performance_object(transaction)

    return delete_keys_from_dict(response)


def construct_contract_response(requested_award_dict):
    """
        Build the Python object to return FPDS Award summary or meta-data via the API

        parameter(s): `requested_award` either award.id (int) or generated_unique_award_id (str)
        returns: an OrderedDict
    """

    response = OrderedDict()
    award = fetch_award_details(requested_award_dict, FPDS_AWARD_FIELDS)
    if not award:
        return None
    response.update(award)

    response["executive_details"] = create_officers_object(award, FPDS_CONTRACT_FIELDS, "fpds")

    transaction = fetch_fpds_details_by_pk(award["_trx"], FPDS_CONTRACT_FIELDS)

    response["latest_transaction_contract_data"] = transaction
    response["funding_agency"] = fetch_agency_details(response["_funding_agency"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency"])
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
    transaction["_lei"] = award["_lei"]
    response["recipient"] = create_recipient_object(transaction)
    response["place_of_performance"] = create_place_of_performance_object(transaction)

    return delete_keys_from_dict(response)


def construct_idv_response(requested_award_dict):
    """
        Build the Python object to return FPDS IDV summary or meta-data via the API

        parameter(s): `requested_award` either award.id (int) or generated_unique_award_id (str)
        returns: an OrderedDict
    """

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

    parent_award = fetch_parent_award_details(award["generated_unique_award_id"])

    response["executive_details"] = create_officers_object(award, mapper, "fpds")

    transaction = fetch_fpds_details_by_pk(award["_trx"], mapper)

    response["parent_award"] = parent_award
    response["parent_generated_unique_award_id"] = parent_award["generated_unique_award_id"] if parent_award else None
    response["latest_transaction_contract_data"] = transaction
    response["funding_agency"] = fetch_agency_details(response["_funding_agency"])
    if response["funding_agency"]:
        response["funding_agency"]["office_agency_name"] = transaction["_funding_office_name"]
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency"])
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
    transaction["_lei"] = award["_lei"]
    response["recipient"] = create_recipient_object(transaction)
    response["place_of_performance"] = create_place_of_performance_object(transaction)

    return delete_keys_from_dict(response)


def create_recipient_object(db_row_dict):
    return OrderedDict(
        [
            (
                "recipient_hash",
                obtain_recipient_uri(
                    db_row_dict["_recipient_name"],
                    db_row_dict["_recipient_unique_id"],
                    db_row_dict["_parent_recipient_unique_id"],
                ),
            ),
            ("recipient_name", db_row_dict["_recipient_name"]),
            ("recipient_unique_id", db_row_dict["_recipient_unique_id"]),
            (
                "parent_recipient_hash",
                obtain_recipient_uri(
                    db_row_dict["_parent_recipient_name"],
                    db_row_dict["_parent_recipient_unique_id"],
                    None,  # parent_recipient_unique_id
                    True,  # is_parent_recipient
                ),
            ),
            ("parent_recipient_name", db_row_dict["_parent_recipient_name"]),
            ("parent_recipient_unique_id", db_row_dict["_parent_recipient_unique_id"]),
            (
                "business_categories",
                get_business_category_display_names(fetch_business_categories_by_legal_entity_id(db_row_dict["_lei"])),
            ),
            (
                "location",
                OrderedDict(
                    [
                        ("location_country_code", db_row_dict["_rl_location_country_code"]),
                        ("country_name", db_row_dict["_rl_country_name"]),
                        ("state_code", db_row_dict["_rl_state_code"]),
                        ("city_name", db_row_dict["_rl_city_name"]),
                        ("county_name", db_row_dict["_rl_county_name"]),
                        ("address_line1", db_row_dict["_rl_address_line1"]),
                        ("address_line2", db_row_dict["_rl_address_line2"]),
                        ("address_line3", db_row_dict["_rl_address_line3"]),
                        ("congressional_code", db_row_dict["_rl_congressional_code"]),
                        ("zip4", db_row_dict["_rl_zip4"]),
                        ("zip5", db_row_dict["_rl_zip5"]),
                        ("foreign_postal_code", db_row_dict.get("_rl_foreign_postal_code")),
                        ("foreign_province", db_row_dict.get("_rl_foreign_province")),
                    ]
                ),
            ),
        ]
    )


def create_place_of_performance_object(db_row_dict):
    return OrderedDict(
        [
            ("location_country_code", db_row_dict["_pop_location_country_code"]),
            ("country_name", db_row_dict["_pop_country_name"]),
            ("county_name", db_row_dict["_pop_county_name"]),
            ("city_name", db_row_dict["_pop_city_name"]),
            (
                "state_code",
                db_row_dict["_pop_state_code"]
                if db_row_dict["_pop_state_code"]
                else state_code_from_name(db_row_dict["_pop_state_name"]),
            ),
            (
                "state_name",
                db_row_dict["_pop_state_name"]
                if db_row_dict["_pop_state_name"]
                else state_name_from_code(db_row_dict["_pop_state_code"]),
            ),
            ("congressional_code", db_row_dict["_pop_congressional_code"]),
            ("zip4", db_row_dict["_pop_zip4"]),
            ("zip5", db_row_dict["_pop_zip5"]),
            ("address_line1", None),
            ("address_line2", None),
            ("address_line3", None),
            ("foreign_province", db_row_dict.get("_pop_foreign_province")),
            ("foreign_postal_code", None),
        ]
    )


def create_officers_object(award, mapper, transaction_type):

    transaction = fetch_latest_ec_details(award["id"], mapper, transaction_type)

    officers = []

    if transaction:
        for officer_num in range(1, 6):
            officer_name_key = "_officer_{}_name".format(officer_num)
            officer_amount_key = "_officer_{}_amount".format(officer_num)
            officer_name = transaction.get(officer_name_key)
            officer_amount = transaction.get(officer_amount_key)
            if officer_name or officer_amount:
                officers.append({"name": officer_name, "amount": officer_amount})

    return {"officers": officers}


def fetch_award_details(filter_q, mapper_fields):
    vals, ann = split_mapper_into_qs(mapper_fields)
    return Award.objects.filter(**filter_q).values(*vals).annotate(**ann).first()


def fetch_parent_award_details(guai):
    parent_award_ids = (
        ParentAward.objects.filter(generated_unique_award_id=guai, parent_award__isnull=False)
        .values("parent_award__award_id", "parent_award__generated_unique_award_id")
        .first()
    )

    if not parent_award_ids:
        return None

    parent_award = (
        Award.objects.filter(id=parent_award_ids["parent_award__award_id"])
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
        logging.debug("Unable to find award for award id %s" % parent_award_ids["parent_award__award_id"])
        return None
    parent_agency = (
        SubtierAgency.objects.filter(subtier_code=parent_award["latest_transaction__contract_data__agency_id"])
        .values("name")
        .first()
    )

    parent_object = OrderedDict(
        [
            ("agency_id", parent_award["latest_transaction__contract_data__agency_id"]),
            ("agency_name", parent_agency["name"]),
            ("award_id", parent_award_ids["parent_award__award_id"]),
            ("generated_unique_award_id", parent_award_ids["parent_award__generated_unique_award_id"]),
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


def fetch_fabs_details_by_pk(primary_key, mapper):
    vals, ann = split_mapper_into_qs(mapper)
    return TransactionFABS.objects.filter(pk=primary_key).values(*vals).annotate(**ann).first()


def fetch_fpds_details_by_pk(primary_key, mapper):
    vals, ann = split_mapper_into_qs(mapper)
    return TransactionFPDS.objects.filter(pk=primary_key).values(*vals).annotate(**ann).first()


def fetch_latest_ec_details(award_id, mapper, transaction_type):
    vals, ann = split_mapper_into_qs(mapper)
    model = TransactionFPDS if transaction_type == "fpds" else TransactionFABS
    retval = (
        model.objects.filter(transaction__award_id=award_id, officer_1_name__isnull=False)
        .values(*vals)
        .annotate(**ann)
        .order_by("-action_date")
    )
    return retval.first()


def fetch_agency_details(agency_id):
    values = [
        "toptier_agency__cgac_code",
        "toptier_agency__name",
        "toptier_agency__abbreviation",
        "subtier_agency__subtier_code",
        "subtier_agency__name",
        "subtier_agency__abbreviation",
    ]
    agency = Agency.objects.filter(pk=agency_id).values(*values).first()

    agency_details = None
    if agency:
        agency_details = {
            "id": agency_id,
            "toptier_agency": {
                "name": agency["toptier_agency__name"],
                "code": agency["toptier_agency__cgac_code"],
                "abbreviation": agency["toptier_agency__abbreviation"],
            },
            "subtier_agency": {
                "name": agency["subtier_agency__name"],
                "code": agency["subtier_agency__subtier_code"],
                "abbreviation": agency["subtier_agency__abbreviation"],
            },
        }
    return agency_details


def fetch_business_categories_by_legal_entity_id(legal_entity_id):
    le = LegalEntity.objects.filter(pk=legal_entity_id).values("business_categories").first()

    if le:
        return le["business_categories"]
    return []


def fetch_all_cfda_details(award):
    queryset = TransactionFABS.objects.filter(transaction__award_id=award["id"]).values(
        "cfda_number", "federal_action_obligation", "non_federal_funding_amount", "total_funding_amount"
    )
    cfdas = {}
    for item in queryset:
        # sometimes the transactions data has the trailing 0 in the CFDA number truncated, this adds it back
        cfda_number = item.get("cfda_number")
        if cfda_number and len(cfda_number) < 6:
            cfda_number += "0" * (6 - len(cfda_number))
        if cfdas.get(cfda_number):
            cfdas.update(
                {
                    cfda_number: {
                        "federal_action_obligation": cfdas[cfda_number]["federal_action_obligation"]
                        + Decimal(item["federal_action_obligation"] or 0),
                        "non_federal_funding_amount": cfdas[cfda_number]["non_federal_funding_amount"]
                        + Decimal(item["non_federal_funding_amount"] or 0),
                        "total_funding_amount": cfdas[cfda_number]["total_funding_amount"]
                        + Decimal(item["total_funding_amount"] or 0),
                    }
                }
            )
        else:
            cfdas.update(
                {
                    cfda_number: {
                        "federal_action_obligation": Decimal(item["federal_action_obligation"] or 0),
                        "non_federal_funding_amount": Decimal(item["non_federal_funding_amount"] or 0),
                        "total_funding_amount": Decimal(item["total_funding_amount"] or 0),
                    }
                }
            )

    c = []
    for cfda_number in cfdas.keys():
        details = fetch_cfda_details_using_cfda_number(cfda_number)
        if details.get("url") == "None;":
            details.update({"url": None})
        c.append(
            {
                "cfda_number": cfda_number,
                "federal_action_obligation_amount": cfdas[cfda_number]["federal_action_obligation"],
                "non_federal_funding_amount": cfdas[cfda_number]["non_federal_funding_amount"],
                "total_funding_amount": cfdas[cfda_number]["total_funding_amount"],
                "cfda_title": details.get("program_title"),
                "cfda_popular_name": details.get("popular_name"),
                "cfda_objectives": details.get("objectives"),
                "cfda_federal_agency": details.get("federal_agency"),
                "cfda_website": details.get("website_address"),
                "sam_website": details.get("url"),
                "cfda_obligations": details.get("obligations"),
            }
        )
    c.sort(key=lambda x: x["total_funding_amount"], reverse=True)
    return c


def fetch_cfda_details_using_cfda_number(cfda):
    c = (
        Cfda.objects.filter(program_number=cfda)
        .values(
            "program_title", "objectives", "federal_agency", "website_address", "url", "obligations", "popular_name"
        )
        .first()
    )
    if not c:
        return {}
    return c


def fetch_transaction_obligated_amount_by_internal_award_id(internal_award_id):
    _sum = FinancialAccountsByAwards.objects.filter(award_id=internal_award_id).aggregate(
        Sum("transaction_obligated_amount")
    )
    if _sum:
        return _sum.get("transaction_obligated_amount__sum")
