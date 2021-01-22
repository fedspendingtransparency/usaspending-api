import json
from usaspending_api.common.exceptions import UnprocessableEntityException, InvalidParameterException
from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    contract_subaward_mapping,
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_subaward_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
    procurement_type_mapping,
)
from usaspending_api.common.helpers.orm_helpers import award_types_are_valid_groups, subaward_types_are_valid_groups
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    contracts_mapping,
    idv_mapping,
    loan_mapping,
    non_loan_assist_mapping,
)


def alias_response(field_to_alias_dict, results):
    results_copy = results.copy()
    for result in results_copy:
        for field in field_to_alias_dict:
            if field in result:
                value = result[field]
                del result[field]
                result[field_to_alias_dict[field]] = value
    return results_copy


def raise_if_award_types_not_valid_subset(award_type_codes, is_subaward=False):
    """Test to ensure the award types are a subset of only one group.

    For Awards: contracts, idvs, direct_payments, loans, grants, other assistance
    For Sub-Awards: procurement, assistance

    If the types are not a valid subset:
        Raise API exception with a JSON response describing the award type groupings
    """
    msg_head = '"message": "\'award_type_codes\' must only contain types from one group.","award_type_groups": '
    if is_subaward:
        if not subaward_types_are_valid_groups(award_type_codes):
            # Return a JSON response describing the award type groupings
            error_msg = ('{{{} {{ "procurement": {}, "assistance": {}}}}}').format(
                msg_head, json.dumps(procurement_type_mapping), json.dumps(assistance_type_mapping)
            )
            raise UnprocessableEntityException(json.loads(error_msg))

    else:
        if not award_types_are_valid_groups(award_type_codes):
            error_msg = (
                "{{{} {{"
                '"contracts": {},'
                '"loans": {},'
                '"idvs": {},'
                '"grants": {},'
                '"other_financial_assistance": {},'
                '"direct_payments": {}}}}}'
            ).format(
                msg_head,
                json.dumps(contract_type_mapping),
                json.dumps(loan_type_mapping),
                json.dumps(idv_type_mapping),
                json.dumps(grant_type_mapping),
                json.dumps(direct_payment_type_mapping),
                json.dumps(other_type_mapping),
            )
            raise UnprocessableEntityException(json.loads(error_msg))


def raise_if_sort_key_not_valid(sort_key, field_list, is_subaward=False):
    """Test to ensure sort key is present for the group of Awards or Sub-Awards

    Raise API exception if sort key is not present
    """
    msg_prefix = ""
    if is_subaward:
        msg_prefix = "Sub-"
        field_external_name_list = list(contract_subaward_mapping.keys()) + list(grant_subaward_mapping.keys())
    else:
        field_external_name_list = (
            list(contracts_mapping.keys())
            + list(loan_mapping.keys())
            + list(non_loan_assist_mapping.keys())
            + list(idv_mapping.keys())
        )

    if sort_key not in field_external_name_list:
        raise InvalidParameterException(
            "Sort value '{}' not found in {}Award mappings: {}".format(sort_key, msg_prefix, field_external_name_list)
        )

    if sort_key not in field_list:
        raise InvalidParameterException(
            "Sort value '{}' not found in requested fields: {}".format(sort_key, field_list)
        )
