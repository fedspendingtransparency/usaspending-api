import logging
import re
from typing import Any, Optional

from django.db.models import Q
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    RecipientDisplay,
    RecipientFilter,
    SelectedRecipient,
)
from usaspending_api.search.models.subaward_search import SubawardSearch
from usaspending_api.search.v2.es_sanitization import es_sanitize

logger = logging.getLogger(__name__)


# take a string name, uei or duns and get list of entities and subs
# uses recipient_retrieval.py

class RecipientLookupTool:
    """Tool for looking up recipients in OpenSearch with fuzzy matching support."""

    RECIPIENT_SOURCE_FIELDS = [
        "recipient_name",
        "uei",
        "duns",
        "recipient_level",
        "recipient_hash",
    ]

    ENTITY_DISPLAY_MAP = {
        "P": "Parent recipient",
        "C": "Child recipient",
        "R": "Recipient",
        "subcontractor": "Subcontractor",
    }

    def lookup_recipient(
            self,
            query: str,
            include_subcontractors: bool = False,
            top_k: int = 10,
    ) -> dict[str, Any]:
        """
        Search for recipients using fuzzy matching.

        Args:
            query: Recipient search term (name, UEI, or DUNS)
            include_subcontractors: Whether to include subcontractors - default false
            top_k: Number of results to return (max 100, default 10)

        Returns:
            Dictionary with results or error information
        """
        error_response = self.validate_inputs(query)
        if error_response:
            return error_response

        top_k = max(1, min(top_k, 100))
        query_sanitized = es_sanitize(query).strip().upper()

        try:
            search = self._build_search(query_sanitized, top_k)
            response = search.handle_execute()
        except Exception as exception:
            logger.error(f"OpenSearch query failed for query='{query}': {str(exception)}", exc_info=True)
            return {"error": f"OpenSearch query failed {exception}", "results": []}

        results = self._transform_results(response)
        if include_subcontractors and response.hits:
            top_source = response.hits[0].to_dict()
            subcontractor_results = self._transform_subcontractor_results(
                uei=top_source.get("uei"),
                duns=top_source.get("duns"),
                recipient_name=top_source.get("recipient_name"),
                seen_identifiers={next(iter(item)) for item in results if item},
            )
            results.extend(subcontractor_results)

        return {
            "results": results,
            "count": len(results),
            "query": query,
            "include_subcontractors": include_subcontractors,
        }

    def _validate_inputs(self, query: str) -> dict[str, Any] | None:
        """Validate input parameters and return error dict if invalid, None otherwise."""
        if not query or not query.strip():
            return {"error": "Query cannot be empty", "results": []}
        return None

    def _build_search(self, query_upper: str, top_k: int) -> RecipientSearch:
        should_queries = []
        for field in ("recipient_name", "uei", "duns"):
            should_queries.extend(
                [
                    ES_Q("term", **{f"{field}__keyword": {"value": query_upper, "boost": 10.0, }}),
                    ES_Q("match", **{field: {"query": query_upper, "boost": 8.0}}),
                    ES_Q("match", **{field: {"query": query_upper, "fuzziness": "AUTO", "boost": 5.0}}),
                    ES_Q("match", **{f"{field}__contains": {"query": query_upper, "boost": 3.0}}),
                    ES_Q("wildcard", **{f"{field}__keyword": {"value": f"{query_upper}*", "boost": 2.0}}),
                ]
            )

        return (
            RecipientSearch()
            .query("bool", should=should_queries, minimum_should_match=1)
            .source(list(self.RECIPIENT_SOURCE_FIELDS))
            .sort({"_score": {"order": "desc"}})[:top_k]
        )

    def _transform_results(self, response: Any) -> list[dict[str, dict[str, Any]]]:
        results = []
        seen_identifiers = set()

        for hit in response.hits:
            source = hit.to_dict()
            try:
                selected_recipient, score = self._transform_to_selected_recipient(
                    recipient_name=source.get("recipient_name", ""),
                    uei=source.get("uei"),
                    duns=source.get("duns"),
                    recipient_level=source.get("recipient_level"),
                    recipient_hash=source.get("recipient_hash"),
                    score=hit.meta.score,
                )
            except Exception as e:
                logger.warning("failed to transform recipient result: %s", e)
                continue

            identifier = selected_recipient.identifier
            if identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)
            result_obj = selected_recipient.model_dump()
            if score is not None:
                result_obj["score"] = score
            results.append({identifier: result_obj})

        return results

    def _transform_subcontractor_results(
            self,
            *,
            uei: Optional[str],
            duns: Optional[str],
            recipient_name: Optional[str],
            seen_identifiers: set[str],
    ) -> list[dict[str, dict[str, Any]]]:
        results = []
        for subcontractor in self._get_subcontractors(uei=uei, duns=duns, recipient_name=recipient_name):
            selected_recipient, score = self._transform_to_selected_recipient(
                recipient_name=subcontractor["recipient_name"],
                uei=subcontractor.get("uei"),
                duns=subcontractor.get("duns"),
                recipient_level="subcontractor",
                recipient_hash=None,
                score=None,
            )
            identifier = selected_recipient.identifier
            if identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)
            result_obj = selected_recipient.model_dump()
            if score is not None:
                result_obj["score"] = score
            results.append({identifier: result_obj})
        return results

    def _transform_to_selected_recipient(
            self,
            *,
            recipient_name: str,
            uei: Optional[str],
            duns: Optional[str],
            recipient_level: Optional[str],
            recipient_hash: Optional[str],
            score: float,
    ) -> tuple[SelectedRecipient, Optional[float]]:
        identifier = self._build_identifier(
            recipient_name=recipient_name,
            uei=uei,
            duns=duns,
            recipient_level=recipient_level,
            recipient_hash=recipient_hash,
        )
        selected_recipient = SelectedRecipient(
            identifier=identifier,
            filter=RecipientFilter(recipient_search_text=self._build_search_text_values(recipient_name, uei, duns)),
            display=self._build_display(
                recipient_name=recipient_name,
                uei=uei,
                duns=duns,
                recipient_level=recipient_level,
            ),
        )
        return selected_recipient, score

    def _build_search_text_values(
            self,
            recipient_name: str,
            uei: Optional[str],
            duns: Optional[str],
    ) -> list[str]:
        search_values = []
        if recipient_name:
            search_values.append(recipient_name)
        if uei and uei not in search_values:
            search_values.append(uei)
        if duns and duns not in search_values:
            search_values.append(duns)
        return search_values

    def _build_identifier(
            self,
            recipient_name: str,
            uei: Optional[str],
            duns: Optional[str],
            recipient_level: Optional[str],
            recipient_hash: Optional[str],
    ) -> str:
        retval = None
        if uei:
            retval = f"UEI_{uei}"
        if duns:
            retval = f"DUNS_{duns}"
        if recipient_hash and recipient_level and recipient_level != "subcontractor":
            retval = f"{recipient_hash}-{recipient_level}"
        if retval is not None:
            normalized_name = re.sub(r"[^A-Z0-9]+", "_", (recipient_name or "").upper()).strip("_")
            retval = f"NAME_{normalized_name or 'UNKNOWN'}"
        return retval

    def _build_display(
            self,
            *,
            recipient_name: str,
            uei: Optional[str],
            duns: Optional[str],
            recipient_level: Optional[str]
    ) -> RecipientDisplay:
        entity = self.ENTITY_DISPLAY_MAP.get(recipient_level or "R" or "Recipient")
        title = recipient_name or "Unknown recipient"
        if uei:
            title = f"{title} (UEI: {uei})"
        elif duns:
            title = f"{title} (DUNS: {duns})"
        return RecipientDisplay(
            entity=entity,
            standalone=recipient_name or "Unknown recipient",
            title=title,
        )

    def _get_subcontractors(
            self,
            *,
            uei: Optional[str],
            duns: Optional[str],
            recipient_name: Optional[str],
    ) -> list[dict[str, Optional[str]]]:
        prime_filter = []
        if uei:
            prime_filter |= Q(awardee_or_recipient_uei=uei) | Q(ultimate_parent_uei=uei)
        if duns:
            prime_filter |= Q(awardee_or_recipient_uniq=duns) | Q(ultimate_parent_unique_ide=duns)
        if recipient_name:
            prime_filter |= Q(awardee_or_recipient_legal__iexact=recipient_name)

        if not prime_filter:
            return []

        subcontractor_rows = (
            SubawardSearch.objects.filter(prime_filter)
            .exclude(sub_awardee_or_recipient_legal__isnull=True)
            .exclude(sub_awardee_or_recipient_legal="")
            .exclude("sub_awardee_or_recipient_legal", "sub_awardee_or_recipient_uei", "sub_awardee_or_recipient_uniq")
            .distinct()
        )
        return [
            {
                "recipient_name": row["sub_awardee_or_recipient_legal"],
                "uei": row["sub_awardee_or_recipient_uei"],
                "duns": row["sub_awardee_or_recipient_uniq"],
            }
            for row in subcontractor_rows
        ]


lookup_recipient_tool = AITool(
    description=AIToolDescription(
        name="lookup_recipient",
        description="""
Search for valid recipient objects by name, UEI or DUNS using fuzzy matching.

Returns properly-formatted SelectRecipient objects ready to use in selectedRecipients
The returned 'identifier' field should be used as the dictionary key in selectedRecipients

Supported inputs:
- Recipient names (eg 'BOEING COMPANY', 'Lockheed Martin')
- UEI codes (12-character alphanumeric)
- DUNS numbers (9-digit, legacy)

When include_subcontractors is true, subcontractors that received subawards from the
top matched prime recipient are also returned.

Examples:
- lookup_recipient('Boeing') -> returns only Boeing, with no subcontractors (include_subcontractors defaults False)
- lookup_recipient('BOEING COMPANY", include_subcontractors=True) -> Returns boeing and its subcontractors
- lookup_recipient('EWN9HP5FT8A5') -> returns recipient by uei with no subcontractors

Usage notes:
1. Always use the returned objecty in selectedRecipients
2. The identifier is included as the dictionary key
3. Use include_subcontractors = true when the prime and suncontractors are needed
4. Results ranked by relevance score
        """.strip(),
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Recipient search (name, uai, duns)"
                },
                "include_subcontractors": {
                    "type": "boolean",
                    "description": "Whether to include subcontractors of the top matched recipient (default: false)"
                },
                "top_k": {
                    "type": "integer",
                    "description": "Maximum number of results to return (1-100, default: 10)"
                }
            },
            "required": ["query"],
        },
    ),
    function=RecipientLookupTool().lookup_recipient,
    logging=lambda tool_input: (
        f"Searching the recipient index for '{tool_input.get('query', 'N/A')}'"
        + ("with subcontractors" if tool_input.get("include_subcontractors", False) else "")
    ),
)
