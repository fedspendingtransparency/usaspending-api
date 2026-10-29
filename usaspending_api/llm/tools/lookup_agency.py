from dataclasses import dataclass

from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F, Q, QuerySet
from django.db.models.functions import Greatest, Least, Upper
from pgvector.django import CosineDistance

from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import AITool, AIToolDescription
from usaspending_api.llm.tools.exceptions import AgencyNotRecognizedError
from usaspending_api.references.models.agency import Agency
from usaspending_api.search.models.mv_agency_autocomplete import AgencyAutocompleteMatview


@dataclass
class AgencyResult:
    agency_id: int
    toptier_flag: bool
    toptier_code: str
    toptier_abbreviation: str | None
    toptier_name: str
    subtier_abbreviation: str | None
    subtier_name: str | None
    score: float
    matched_query: str


class AgencyLookupTool:
    HYBRID_TEXT_WEIGHT = 0.5
    HYBRID_VECTOR_WEIGHT = 0.5
    VECTOR_DISTANCE_THRESHOLD = 0.75

    SEMANTIC_ONLY_INSTRUCTIONS = (
        "STOP AND READ: no exact or prefix match was found for this query. The results below are weak "
        "semantic-similarity guesses and may be completely wrong. Many entities that sound like government "
        "agencies -- state, local, tribal, or foreign government bodies, universities, hospitals, and "
        "companies -- are RECIPIENTS of federal funding, not federal agencies. "
        "Do NOT select one of these results and do NOT tell the user there is no federal data unless you "
        "have first called lookup_recipient with this entity's name and confirmed it has no matches either."
    )

    NO_MATCH_INSTRUCTIONS = (
        "STOP AND READ: no federal agency matched this query at all. Do NOT tell the user there is no "
        "federal funding data for this entity yet. Many entities that sound like government agencies -- "
        "state, local, tribal, or foreign government bodies, universities, hospitals, and companies -- are "
        "RECIPIENTS of federal funding, not federal agencies. You MUST call lookup_recipient with this "
        "entity's name before concluding there is no data or ending the conversation."
    )

    async def lookup_agencies(self, query: str, top_k: int = 10) -> dict:
        normalized = query.strip()
        matches = await self._query_exact_and_prefix_matches(normalized)

        if await matches.aexists():
            ordered = matches.order_by("-toptier_flag", Upper("toptier_name"), Upper("subtier_name"))
            entries = [self._matview_row_to_entry(row) async for row in ordered[:top_k]]
            semantic_only = False
        else:
            model = await AIModel.objects.aget(name="titan")
            embedding = await EmbeddingGenerator(model=model).agenerate_embedding(normalized)
            hybrid_matches = self._hybrid_search(normalized, embedding)
            entries = [self._agency_row_to_entry(row) async for row in hybrid_matches[:top_k]]
            semantic_only = True

        if not entries:
            user_message = f"Unable to filter. We couldn't find an agency with the name: {query}"
            raise AgencyNotRecognizedError(user_message, model_detail=f"{user_message}. {self.NO_MATCH_INSTRUCTIONS}")

        results = {f"{entry['id']}_{entry['agencyType']}": entry for entry in entries}
        return {
            "results": results,
            "instructions": self.SEMANTIC_ONLY_INSTRUCTIONS if semantic_only else None,
        }

    @staticmethod
    async def _query_exact_and_prefix_matches(query: str) -> QuerySet:
        exact_filter = (
            Q(toptier_code__iexact=query)
            | Q(toptier_abbreviation__iexact=query)
            | Q(toptier_name__iexact=query)
            | Q(subtier_abbreviation__iexact=query)
            | Q(subtier_name__iexact=query)
        )
        exact_matches = AgencyAutocompleteMatview.objects.filter(exact_filter)
        if await exact_matches.aexists():
            return exact_matches

        prefix_filter = (
            Q(toptier_abbreviation__istartswith=query)
            | Q(toptier_name__istartswith=query)
            | Q(subtier_abbreviation__istartswith=query)
            | Q(subtier_name__istartswith=query)
        )
        return AgencyAutocompleteMatview.objects.filter(prefix_filter)

    def _hybrid_search(self, query: str, embedding: list[float]) -> QuerySet:
        text_score = Greatest(
            TrigramSimilarity("toptier_agency__name", query),
            TrigramSimilarity("toptier_agency__abbreviation", query),
            TrigramSimilarity("subtier_agency__name", query),
            TrigramSimilarity("subtier_agency__abbreviation", query),
        )
        vector_distance = Least(
            CosineDistance("toptier_agency__embedding", embedding),
            CosineDistance("subtier_agency__embedding", embedding),
        )

        return (
            Agency.objects.select_related("toptier_agency", "subtier_agency")
            .annotate(text_score=text_score, vector_distance=vector_distance)
            .filter(vector_distance__lt=self.VECTOR_DISTANCE_THRESHOLD)
            .annotate(
                hybrid_score=(self.HYBRID_TEXT_WEIGHT * F("text_score"))
                + (self.HYBRID_VECTOR_WEIGHT * (1 - F("vector_distance")))
            )
            .order_by("-hybrid_score")
        )

    @staticmethod
    def _matview_row_to_entry(row: AgencyAutocompleteMatview) -> dict:
        agency_type = "toptier" if row.toptier_flag else "subtier"
        entry = {
            "id": row.agency_autocomplete_id,
            "agencyType": agency_type,
            "toptier_flag": row.toptier_flag,
            "toptier_agency": {
                "id": row.toptier_agency_id,
                "toptier_code": row.toptier_code,
                "abbreviation": row.toptier_abbreviation,
                "name": row.toptier_name,
            },
        }
        if agency_type == "subtier":
            entry["subtier_agency"] = {"abbreviation": row.subtier_abbreviation, "name": row.subtier_name}
        return entry

    @staticmethod
    def _agency_row_to_entry(row: Agency) -> dict:
        agency_type = "toptier" if row.toptier_flag else "subtier"
        entry = {
            "id": row.id,
            "agencyType": agency_type,
            "toptier_flag": row.toptier_flag,
            "toptier_agency": {
                "id": row.toptier_agency_id,
                "toptier_code": row.toptier_agency.toptier_code,
                "abbreviation": row.toptier_agency.abbreviation,
                "name": row.toptier_agency.name,
            },
        }
        if agency_type == "subtier" and row.subtier_agency:
            entry["subtier_agency"] = {
                "abbreviation": row.subtier_agency.abbreviation,
                "name": row.subtier_agency.name,
            }
        return entry


lookup_agency_tool = AITool(
    function=AgencyLookupTool().lookup_agencies,
    logging=lambda tool_input: f"Searching agencies for '{tool_input['query']}'",
    description=AIToolDescription(
        name="lookup_agencies",
        description="""
            Look up federal awarding/funding agencies using a mix of exact matching, prefix
            matching, and semantic similarity search.

            Agencies may be toptier (e.g. departments and independent agencies) or subtier
            (e.g. sub-agencies, bureaus, and offices within a toptier agency). Each result
            includes the toptier agency; subtier results also include the specific subtier
            agency that matched.

            Returns a dictionary with two keys:
                {
                    "results": {
                        "803_toptier": {
                            "id": 803,
                            "agencyType": "toptier",           # or "subtier"
                            "toptier_flag": true,
                            "toptier_agency": {"id": 66, "toptier_code": "073", "abbreviation": "SBA", "name": "..."},
                            "subtier_agency": {"abbreviation": "...", "name": "..."}   # only for subtier results
                        },
                        ...
                    },
                    "instructions": null   # or a string -- READ THIS if present, see below
                }
            "results" is keyed by "{id}_{agencyType}" (e.g. "803_toptier").

            Matching behavior (in priority order):
            1. Exact match: if the query exactly matches an agency's toptier code,
               abbreviation, or name (case-insensitive), only exact matches are returned
               immediately, skipping semantic search.
            2. Prefix match: if the query is a prefix of one or more agency abbreviations or
               names, those agencies are returned.
            3. Hybrid search: otherwise, the query is matched against agency names and
               abbreviations using trigram text similarity, combined with semantic similarity
               from generated text embeddings over agency name, abbreviation, mission, and
               background information.

            Results are ordered toptier agencies first, then alphabetically.

            "instructions" is non-null whenever the match came from semantic search only (no exact or
            prefix hit) -- this means the results are weak guesses, and the entity may not be a federal
            agency at all (it could be a recipient of federal funds instead, e.g. a state, local, tribal,
            or foreign government body, a university, hospital, or company). When this happens, you MUST
            call lookup_recipient before selecting a result or concluding there's no federal data. The same
            applies if this tool raises an error for zero matches -- do not give up without trying
            lookup_recipient first.

            Examples:
            - lookup_agencies('NASA') -> Exact match, returns the National Aeronautics and
              Space Administration.
            - lookup_agencies('080') -> Exact match on toptier_code, returns NASA.
            - lookup_agencies('Nat') -> Prefix match, returns agencies whose name or
              abbreviation starts with 'Nat' (e.g. NARA, NCPC, NCUA, NEA, NEH...).
            - lookup_agencies('space exploration') -> Semantic match, returns agencies related
              to space exploration even without literal keyword overlap.
            - lookup_agencies('food and nutrition') -> Semantic match, may return USDA subtier
              agencies like NIFA or NASS if their mission/description aligns.
        """,
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Agency name, abbreviation, or code to search for"},
                "top_k": {"type": "integer", "description": "Number of results to return (default: 20)", "default": 20},
            },
            "required": ["query"],
        },
    ),
)
