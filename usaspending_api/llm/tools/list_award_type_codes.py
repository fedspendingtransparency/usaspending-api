from usaspending_api.awards.v2.lookups.lookups import (
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
)
from usaspending_api.llm.models.py_models import AITool, AIToolDescription

_AWARD_TYPE_GROUPS = {
    "contracts": contract_type_mapping,
    "loans": loan_type_mapping,
    "idvs": idv_type_mapping,
    "grants": grant_type_mapping,
    "other_financial_assistance": direct_payment_type_mapping,
    "direct_payments": other_type_mapping,
}

AWARD_TYPE_CODES: dict[str, list[dict[str, str]]] = {
    group: [{"code": code, "label": label} for code, label in mapping.items()]
    for group, mapping in _AWARD_TYPE_GROUPS.items()
}


def list_award_type_codes() -> dict[str, list[dict[str, str]]]:
    return AWARD_TYPE_CODES


list_award_type_codes_tool = AITool(
    function=list_award_type_codes,
    logging=lambda _: "Listing award type codes.",
    description=AIToolDescription(
        name="list_award_type_codes",
        description=(
            "List valid award-type codes grouped by category. Takes no input; returns {code, label} "
            "entries. An 'awardType' filter may only contain codes from a SINGLE group."
        ),
        input_schema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
)
