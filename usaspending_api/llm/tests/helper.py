from typing import Any

from usaspending_api.common.elasticsearch.search_wrappers import AsyncRecipientSearch
from usaspending_api.llm.tools.lookup_recipient import RecipientLookupTool
from usaspending_api.search.v2.es_sanitization import es_sanitize

_tool = RecipientLookupTool()


def build_fuzzy_recipient_query(search_text: str) -> AsyncRecipientSearch:
    return _tool._build_search(es_sanitize(search_text).strip().upper(), top_k=10)


async def fuzzy_search_recipients(
    search_text: str,
    limit: int = 10,
) -> list | dict[str, Any]:
    response = await _tool._build_search(es_sanitize(search_text).strip().upper(), top_k=limit).handle_execute()
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


async def retrieve_recipient_names(
    search_text: str,
    limit: int = 5,
) -> dict[str, list[str]]:
    return await _tool.lookup_recipient(search_text, top_k=limit)
