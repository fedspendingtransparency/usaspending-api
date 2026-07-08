from typing import Any

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.tools.lookup_recipient import RecipientLookupTool
from usaspending_api.search.v2.es_sanitization import es_sanitize

_tool = RecipientLookupTool()


def build_fuzzy_recipient_query(search_text: str) -> RecipientSearch:
    return _tool._build_search(es_sanitize(search_text).strip().upper(), top_k=10)


def fuzzy_search_recipients(
        search_text: str,
        limit: int = 10,
) -> list | dict[str, Any]:
    response = _tool._build_search(es_sanitize(search_text).strip().upper(), top_k=limit).handle_execute()
    if not response.hits:
        return []
    return [
        {
            "recipient_name": hit.to_dict().get("recipient_name"),
            "uei": hit.to_dict().get("uei"),
            "duns": hit.to_dict().get("duns"),
            "recipient_level": hit.to_dict().get("recipient_level"),
            "recipient_hash": hit.to_dict().get("recipient_hash"),
            "score": hit.meta.score,
        }
        for hit in response.hits
    ]


def expand_prime_recipient_subcontractors(**kwargs) -> dict[str, Any]:
    subcontractors = _tool._get_subcontractors(
        uei=kwargs.get("uei"),
        duns=kwargs.get("duns"),
        recipient_name=kwargs.get("recipient_name"),
    )
    prime = {
        "recipient_name": kwargs.get("recipient_name"),
        "uei": kwargs.get("uei"),
        "duns": kwargs.get("duns"),
        "recipient_hash": kwargs.get("recipient_hash"),
        "recipient_level": kwargs.get("recipient_level"),
    }
    all_names = []
    if prime.get("recipient_name"):
        all_names.append(prime["recipient_name"])
    for subcontractor in subcontractors:
        name = subcontractor.get("recipient_name")
        if name and name not in all_names:
            all_names.append(name)

    return {
        "prime": prime,
        "subcontractors": subcontractors,
        "all_recipient_names": all_names,
    }


def retrieve_company_and_subcontractors(
        search_text: str,
        limit: int = 5,
) -> dict[str, Any]:
    result = _tool.lookup_recipient(
        search_text,
        include_subcontractors=True,
        top_k=limit,
    )
    recipient_names = []
    for item in result.get("results", []):
        recipient_obj = next(iter(item.values()))
        filter_obj = recipient_obj.get("filter", {})
        recipient_names.extend(filter_obj.get("recipient_search_text", []))
    recipient_names = list(dict.fromkeys(recipient_names))
    return {
        "query": search_text,
        "matches": result.get("results", []),
        "recipient_names": recipient_names,
    }
