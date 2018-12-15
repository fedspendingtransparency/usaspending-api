import copy
import logging
from collections import OrderedDict, MutableMapping
from django.db.models import F

from usaspending_api.awards.v2.data_layer.orm_mappers import FPDS_CONTRACT_FIELDS, OFFICER_FIELDS, FPDS_AWARD_FIELDS
from usaspending_api.awards.models import Award, TransactionFPDS, ParentAward
from usaspending_api.references.models import Agency, LegalEntity, LegalEntityOfficers

logger = logging.getLogger("console")


def delete_keys_from_dict(dictionary):
    modified_dict = OrderedDict()
    for key, value in dictionary.items():
        if not key.startswith("_"):
            if isinstance(value, MutableMapping):
                modified_dict[key] = delete_keys_from_dict(value)
            else:
                modified_dict[key] = copy.deepcopy(value)
    return modified_dict


def split_mapper_into_qs(mapper):
    values_list = [k for k, v in mapper.items() if k == v]
    annotate_dict = {v: F(k) for k, v in mapper.items() if k != v}

    return values_list, annotate_dict


def construct_idv_response(requested_award):
    """
        [x] base award data
        [x] awarding_agency
        [x] funding_agency
        [x] recipient
        [x] place_of_performance
        [x] executive_details
        [x] idv_dates
        [x] latest_transaction_contract_data
    """
    f = {"generated_unique_award_id": requested_award}
    if str(requested_award).isdigit():
        f = {"pk": requested_award}

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

    award = fetch_fpds_award(f)
    if not award:
        return None
    response.update(award)

    response["parent_generated_unique_award_id"] = fetch_parent_award_id(award["generated_unique_award_id"])
    response["executive_details"] = fetch_officers_by_legal_entity_id(award["_lei"])
    response["latest_transaction_contract_data"] = fetch_fpds_details_by_pk(award["_trx"], mapper)
    response["funding_agency"] = fetch_agency_details(response["_funding_agency"])
    response["awarding_agency"] = fetch_agency_details(response["_awarding_agency"])
    response["idv_dates"] = OrderedDict(
        [
            ("start_date", response["latest_transaction_contract_data"]["_start_date"]),
            ("last_modified_date", response["latest_transaction_contract_data"]["_last_modified_date"]),
            ("end_date", response["latest_transaction_contract_data"]["_end_date"]),
        ]
    )
    response["recipient"] = OrderedDict(
        [
            ("recipient_name", response["latest_transaction_contract_data"]["_recipient_name"]),
            ("recipient_unique_id", response["latest_transaction_contract_data"]["_recipient_unique_id"]),
            ("parent_recipient_unique_id", response["latest_transaction_contract_data"]["_parent_recipient_unique_id"]),
            ("business_categories", fetch_business_categories_by_legal_entity_id(award["_lei"])),
        ]
    )
    response["place_of_performance"] = OrderedDict(
        [
            ("location_country_code", response["latest_transaction_contract_data"]["_country_code"]),
            ("country_name", response["latest_transaction_contract_data"]["_country_name"]),
            ("county_name", response["latest_transaction_contract_data"]["_county_name"]),
            ("city_name", response["latest_transaction_contract_data"]["_city_name"]),
            ("state_code", response["latest_transaction_contract_data"]["_state_code"]),
            ("congressional_code", response["latest_transaction_contract_data"]["_congressional_code"]),
            ("zip4", response["latest_transaction_contract_data"]["_zip4"]),
            ("zip5", response["latest_transaction_contract_data"]["_zip5"]),
            ("address_line1", None),
            ("address_line2", None),
            ("address_line3", None),
            ("foreign_province", None),
            ("foreign_postal_code", None),
        ]
    )

    return delete_keys_from_dict(response)


def fetch_fpds_award(filter_q):
    vals, ann = split_mapper_into_qs(FPDS_AWARD_FIELDS)
    return Award.objects.filter(**filter_q).values(*vals).annotate(**ann).first()


def fetch_parent_award_id(guai):
    parent_award = (
        ParentAward.objects.filter(generated_unique_award_id=guai)
        .values("parent_award__generated_unique_award_id")
        .first()
    )

    return parent_award.get("generated_unique_award_id") if parent_award else None


def fetch_fpds_details_by_pk(primary_key, mapper):
    vals, ann = split_mapper_into_qs(mapper)
    return TransactionFPDS.objects.filter(pk=primary_key).values(*vals).annotate(**ann).first()


def fetch_agency_details(agency_id):
    values = [
        "toptier_agency__fpds_code",
        "toptier_agency__name",
        "subtier_agency__subtier_code",
        "subtier_agency__name",
        "office_agency__name",
    ]
    agency = Agency.objects.filter(pk=agency_id).values(*values).first()

    agency_details = None
    if agency:
        agency_details = {
            "id": agency_id,
            "toptier_agency": {"name": agency["toptier_agency__name"], "code": agency["toptier_agency__fpds_code"]},
            "subtier_agency": {"name": agency["subtier_agency__name"], "code": agency["subtier_agency__subtier_code"]},
            "office_agency_name": agency["office_agency__name"],
        }
    return agency_details


def fetch_business_categories_by_legal_entity_id(legal_entity_id):
    le = LegalEntity.objects.filter(pk=legal_entity_id).values("business_categories").first()

    if le:
        return le["business_categories"]
    return None


def fetch_officers_by_legal_entity_id(legal_entity_id):
    officer_info = LegalEntityOfficers.objects.filter(pk=legal_entity_id).values(*OFFICER_FIELDS.keys()).first()

    officers = []
    if officer_info:
        for x in range(1, 6):
            officers.append(
                {
                    "name": officer_info["officer_{}_name".format(x)],
                    "amount": officer_info["officer_{}_amount".format(x)],
                }
            )

    return {"officers": officers}
