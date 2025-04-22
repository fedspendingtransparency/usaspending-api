import json

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    contracts_mapping,
    idv_mapping,
    loan_mapping,
    non_loan_assist_mapping,
)
from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
    procurement_type_mapping,
    non_loan_assistance_type_mapping,
    subaward_mapping,
)
from usaspending_api.common.exceptions import UnprocessableEntityException, InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import award_types_are_valid_groups, subaward_types_are_valid_groups
from usaspending_api.search.v2.views.enums import SpendingLevel

INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS = 'Incompatible parameters: `state` must be provided, country must be "USA", and county cannot be provided when using `district_current` or `district_original`.'
DUPLICATE_DISTRICT_LOCATION_PARAMETERS = (
    "Incompatible parameters: `district_current` and `district_original` are not allowed in the same locations object."
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


def raise_if_award_types_not_valid_subset(
    award_type_codes: list[str], spending_level: SpendingLevel = SpendingLevel.AWARD
):
    """Test to ensure the award types are a subset of only one group.

    For Awards: contracts, idvs, direct_payments, loans, grants, other assistance
    For Sub-Awards: procurement, assistance

    If the types are not a valid subset:
        Raise API exception with a JSON response describing the award type groupings
    """
    msg_head = '"message": "\'award_type_codes\' must only contain types from one group.","award_type_groups": '
    if spending_level == SpendingLevel.SUBAWARD:
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


def raise_if_sort_key_not_valid(
    sort_key: str,
    field_list: list[str],
    award_type_codes: list[str],
    spending_level: SpendingLevel = SpendingLevel.AWARD,
):
    """Test to ensure sort key is present for the group of Awards or Sub-Awards

    Raise API exception if sort key is not present
    """
    award_type, field_external_name_list = get_award_type_and_mapping_values(award_type_codes, spending_level)

    if sort_key not in field_external_name_list:
        raise InvalidParameterException(
            "Sort value '{}' not found in {} mappings: {}".format(sort_key, award_type, field_external_name_list)
        )

    if sort_key not in field_list:
        raise InvalidParameterException(
            "Sort value '{}' not found in requested fields: {}".format(sort_key, field_list)
        )


def get_award_type_and_mapping_values(
    award_type_codes: list[str], spending_level: SpendingLevel
) -> tuple[str, list[str]]:
    """Returns a tuple including the type of award and the award mapping values based on the input provided.
    If the input doesn't match any award type mappings then it will raise an exception JSON response.

    Args:
        award_type_codes: List of award type codes
        spending_level: Enum of different spending levels

    Returns:
        A tuple of two items
            - First element is a string that describes the award type
            - Second element is a list of strings that are the values from the corresponding award mapping
    """
    if spending_level == SpendingLevel.SUBAWARD:
        award_type = "Sub-Award"
        award_type_mapping_values = list(subaward_mapping.keys())
    elif set(award_type_codes) <= set(contract_type_mapping):
        award_type = "Contract Award"
        award_type_mapping_values = list(contracts_mapping.keys())
    elif set(award_type_codes) <= set(loan_type_mapping):
        award_type = "Loan Award"
        award_type_mapping_values = list(loan_mapping.keys())
    elif set(award_type_codes) <= set(idv_type_mapping):
        award_type = "IDV Award"
        award_type_mapping_values = list(idv_mapping.keys())
    elif set(award_type_codes) <= set(non_loan_assistance_type_mapping):
        award_type = "Non-Loan Assistance Award"
        award_type_mapping_values = list(non_loan_assist_mapping.keys())
    else:
        raise InvalidParameterException(
            "Award type codes '{}' did not match any award mappings.".format(award_type_codes)
        )

    return award_type, award_type_mapping_values
