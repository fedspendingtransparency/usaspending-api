from dataclasses import dataclass

from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F, Q, QuerySet
from django.db.models.functions import Greatest, Least, Upper
from pgvector.django import CosineDistance

from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    AIToolResult,
    SelectedAgency,
    SubtierAgency,
    ToptierAgency,
)
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

    async def lookup_agencies(self, query: str, top_k: int = 10, agency_type: str = "awarding") -> AIToolResult:
        normalized = query.strip()
        matches = await self._query_exact_and_prefix_matches(normalized)

        if await matches.aexists():
            ordered = matches.order_by("-toptier_flag", Upper("toptier_name"), Upper("subtier_name"))
            entries = [self._matview_row_to_entry(row) async for row in ordered[:top_k]]
        else:
            model = await AIModel.objects.aget(name="titan")
            embedding = await EmbeddingGenerator(model=model).agenerate_embedding(normalized)
            hybrid_matches = self._hybrid_search(normalized, embedding)
            entries = [self._agency_row_to_entry(row) async for row in hybrid_matches[:top_k]]

        if not entries:
            raise AgencyNotRecognizedError(f"Unable to filter. We couldn't find an agency with the name: {query}")

        results = AIToolResult(results={f"{entry.id}_{entry.agencyType}": entry.model_dump() for entry in entries})

        return results

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
    def _matview_row_to_entry(row: AgencyAutocompleteMatview) -> SelectedAgency:
        agency_type = "toptier" if row.toptier_flag else "subtier"
        if agency_type == "subtier":
            subAgency = SubtierAgency(abbreviation=row.subtier_abbreviation, name=row.subtier_name)
        else:
            subAgency = None
        topAgency = ToptierAgency(
            id=row.toptier_agency_id,
            toptier_code=row.toptier_code,
            abbreviation=row.toptier_abbreviation,
            name=row.toptier_name,
        )
        entry = SelectedAgency(
            id=row.agency_autocomplete_id,
            agencyType=agency_type,
            toptier_flag=row.toptier_flag,
            toptier_agency=topAgency,
            subtier_agency=subAgency,
        )

        return entry

    @staticmethod
    def _agency_row_to_entry(row: Agency) -> SelectedAgency:
        agency_type = "toptier" if row.toptier_flag else "subtier"
        if agency_type == "subtier" and row.subtier_agency:
            subAgency = SubtierAgency(abbreviation=row.subtier_agency.abbreviation, name=row.subtier_agency.name)
        else:
            subAgency = None

        topAgency = ToptierAgency(
            id=int(row.toptier_agency.toptier_agency_id),
            toptier_code=row.toptier_agency.toptier_code,
            abbreviation=row.toptier_agency.abbreviation,
            name=row.toptier_agency.name,
        )
        entry = SelectedAgency(
            id=row.id,
            agencyType=agency_type,
            toptier_flag=row.toptier_flag,
            toptier_agency=topAgency,
            subtier_agency=subAgency,
        )
        return entry


lookup_agency_tool = AITool(
    function=AgencyLookupTool().lookup_agencies,
    logging=lambda tool_input: f"Searching agencies for '{tool_input['query']}'",
    description=AIToolDescription(
        name="lookup_agencies",
        description="""
            Look up federal agencies that distribute awards or fund awards
            using a mix of exact matching, prefix matching, and semantic similarity search.

            Agencies may be toptier (e.g. departments and independent agencies) or subtier
            (e.g. sub-agencies, bureaus, and offices within a toptier agency). Each result
            includes the toptier agency; subtier results also include the specific subtier
            agency that matched.

            Returns a Filter model object, with either the "selectedFundingAgencies" or
            "selectedAwardingAgencies" field defined based on the input value of 'agency_type'.
            Each field is a dictionary keyed by "{id}_{agencyType}" (e.g. "803_toptier"),
            where each value is a corresponding selectedAgency model instance

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

            Examples:
            - lookup_agencies('NASA') -> Exact match, returns the National Aeronautics and
              Space Administration.
            - lookup_agencies('080') -> Exact match on toptier_code, returns NASA.
            - lookup_agencies('Nat') -> Prefix match, returns agencies whose name or
              abbreviation starts with 'Nat' (e.g. NARA, NCPC, NCUA, NEA, NEH...).
            - lookup_agencies('space exploration') -> Semantic match, returns agencies related
              to space exploration even without literal keyword overlap.
            - lookup_agencies('food and nutrition') -> Semantic match, may return USDA subtier
              agencies like NIFA or NASS if their description align. This use case should be avoided
              in situations where you are looking for awards in a given field or industry, as
              the using a different tool or no tool at all would be more appropriate.
        """,
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Agency name, abbreviation, or code to search for"},
                "top_k": {"type": "integer", "description": "Number of results to return (default: 20)", "default": 20},
                "agency_type": {
                    "type": "string",
                    "enum": ["awarding", "funding"],
                    "description": (
                        "Optional: Whether an agency should be listed as an awarding agency or a funding agency"
                    ),
                    "default": "awarding",
                },
            },
            "required": ["query"],
        },
    ),
)
