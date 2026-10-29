import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Literal

from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F, FloatField, Model, Q, QuerySet, Value, Window
from django.db.models.functions import Greatest, RowNumber
from pgvector.django import CosineDistance

from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    AIToolResult,
    NaicsCodeLists,
    PscCodeLists,
    SelectedCFDA,
    TasCodeLists,
)
from usaspending_api.llm.tools.exceptions import (
    AssistanceListingNotRecognizedError,
    NaicsCodeNotRecognizedError,
    PscCodeNotRecognizedError,
    TasNotRecognizedError,
)
from usaspending_api.llm.tools.expand_query import expand_query
from usaspending_api.llm.tools.helpers import hierarchy_parsers
from usaspending_api.llm.tools.helpers.code_paths import build_psc_path, build_tas_path
from usaspending_api.references.models import ToptierAgency
from usaspending_api.references.models.cfda import Cfda
from usaspending_api.references.models.naics import NAICS
from usaspending_api.references.models.psc import PSC
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS, PscGroupLabels

logger = logging.getLogger(__name__)

CODE_NOT_RECOGNIZED_ERRORS = {
    "naics": (NaicsCodeNotRecognizedError, "NAICS code not found"),
    "psc": (PscCodeNotRecognizedError, "PSC code not found"),
    "cfda": (AssistanceListingNotRecognizedError, "Assistance listing code not found"),
    "tas": (TasNotRecognizedError, "TAS not found"),
}

# Semantic search always returns its nearest neighbors, however loosely related. Only results scoring at least this
# fraction of the best match are selected, and no more than the maximum number of them.
SEMANTIC_MIN_RELATIVE_SCORE = 0.8
SEMANTIC_MAX_SELECTIONS = 20
# A match must also score at least this much, so a query with nothing related returns an error instead of its single
# nearest neighbor.
SEMANTIC_MIN_ABSOLUTE_SCORE = 0.3
# A more specific code replaces its ancestor only if it scores at least this much higher
SEMANTIC_DESCENDANT_MARGIN = 0.05

# Queries that are a TAS main account ("3200") or AID-MAIN ("014-0102") rather than the start of a rendering label
TAS_MAIN_ACCOUNT_PATTERN = re.compile(r"\d{4}")
TAS_AID_MAIN_PATTERN = re.compile(r"\d{3}-\d{4}")
# Queries beginning with a digit are codes (e.g. "3200", "14.257"); expanding them only produces unrelated variations
NUMERIC_CODE_QUERY_PATTERN = re.compile(r"\d[0-9A-Za-z./-]*")

# A label a llm tends to put in front of a code ("AL 14.257", "NAICS 1111"). It is only stripped when a code follows.
CODE_LABEL_PATTERNS = {
    "cfda": re.compile(r"(?:ALN|AL|CFDA|assistance listings?(?: numbers?)?|program numbers?)\b[\s:#.-]*(?=\d)", re.I),
    "naics": re.compile(r"NAICS(?: codes?)?\b[\s:#.-]*(?=\d)", re.I),
    "psc": re.compile(r"(?:PSC|product and service codes?)\b[\s:#.-]*(?=(?-i:[A-Z0-9]{1,4})\b)", re.I),
    "tas": re.compile(r"(?:TAS|treasury account(?: symbols?)?)\b[\s:#.-]*(?=\d)", re.I),
}
# An assistance listing program number (e.g. "14.257" or "93.00A") inside a longer query
CFDA_PROGRAM_NUMBER_PATTERN = re.compile(r"(?<!\d)\d{2}\.\d{2}[0-9A-Za-z]\b")


@dataclass
class CodeTypeConfig:
    """Configuration for each code type"""

    model_class: type[Model]
    code_field: str
    description_field: str
    get_parent_code: Callable[[str], Any]
    get_all_ancestors: Callable[[str], Any]


# Configuration for each supported code type
CODE_TYPE_CONFIGS = {
    "naics": CodeTypeConfig(
        model_class=NAICS,
        code_field="code",
        description_field="description",
        get_parent_code=hierarchy_parsers.get_naics_parent,
        get_all_ancestors=hierarchy_parsers.get_naics_ancestors,
    ),
    "psc": CodeTypeConfig(
        model_class=PSC,
        code_field="code",
        description_field="description",
        get_parent_code=hierarchy_parsers.get_psc_parent,
        get_all_ancestors=hierarchy_parsers.get_psc_ancestors,
    ),
    "cfda": CodeTypeConfig(
        model_class=Cfda,
        code_field="program_number",
        description_field="program_title",
        get_parent_code=hierarchy_parsers.get_cfda_parent,
        get_all_ancestors=hierarchy_parsers.get_cfda_ancestors,
    ),
    "tas": CodeTypeConfig(
        model_class=TreasuryAppropriationAccount,
        code_field="tas_rendering_label",
        description_field="account_title",
        get_parent_code=hierarchy_parsers.get_tas_parent,
        get_all_ancestors=hierarchy_parsers.get_tas_ancestors,
    ),
}


# The filter model used to validate and format the codes found for each code type (CFDA has no require/exclude lists)
CODE_LISTS_MODELS = {
    "naics": NaicsCodeLists,
    "psc": PscCodeLists,
    "tas": TasCodeLists,
}


@dataclass
class CodeResult:
    """A single matched or ancestor code entry."""

    code: str
    description: str | None
    score: float = 0.0
    matched_query: str | None = None
    budget_bureau_name: str | None = None  # only used for tas results


class SearchResultNode:
    """Node for search results hierarchy"""

    def __init__(
        self,
        code: str,
        description: str,
        score: float,
        all_results: dict[str, CodeResult],
        config: CodeTypeConfig,
        is_root: bool = True,
    ):
        self.code = code
        self.description = description
        self.score = score
        self.children: list[SearchResultNode] = []
        self.config = config

        self._is_root = is_root

        # Build children
        self._populate_children(all_results)

    def _populate_children(self, all_results: dict[str, CodeResult]) -> None:
        """Build children from results that extend this code"""

        for child_code, child_data in all_results.items():
            if self._is_direct_child(child_code):
                child_node = SearchResultNode(
                    code=child_code,
                    description=child_data.description,
                    score=child_data.score,
                    all_results=all_results,
                    config=self.config,
                    is_root=False,
                )
                self.children.append(child_node)

    def _is_direct_child(self, other_code: str) -> bool:
        """Check if other_code is a direct child of this code"""
        if not self.config.get_parent_code:
            return False
        parent = self.config.get_parent_code(other_code)
        return parent == self.code

    def to_dict(self) -> dict:
        """Convert to dictionary format"""

        return {
            "code": self.code,
            "description": self.description,
            "score": self.score,
            "is_root": self.is_root,
            "is_leaf": self.is_leaf,
            "children": {
                child.code: child.to_dict() for child in sorted(self.children, key=lambda x: x.score, reverse=True)
            },
        }

    @property
    def is_leaf(self) -> bool:
        return len(self.children) == 0

    @property
    def is_root(self) -> bool:
        return self._is_root


class CodeLookupTool:
    """Generalized tool for looking up various code types using hybrid text + vector similarity search"""

    async def lookup_codes(
        self,
        query: str,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        inclusion: Literal["require", "exclude"] = "require",
        top_k: int = 20,
        query_fanout: int | None = 3,
    ) -> AIToolResult:
        """
        Hybrid search for various code types combining text matching and vector similarity.

        Returns the matched codes in the shape the execute_filter tool expects for that code type.
        """

        if msg := await self.validate_input(code_type, inclusion):
            return AIToolResult(results={"error": msg})

        config = CODE_TYPE_CONFIGS[code_type]
        budget_bureau_names = {}
        query = self._normalize_query(query, code_type)

        codes = await self._handle_exact_search(query, config, code_type, budget_bureau_names, top_k)
        is_semantic_search = codes is None
        if is_semantic_search:
            codes = await self._handle_hybrid_search(query, config, code_type, budget_bureau_names, top_k, query_fanout)

        # A query that is itself a code in the hierarchy (e.g. the TAS agency "309") selects that code alone, even
        # when the node is only an ancestor of what matched. CFDA is excluded as only its programs can be selected.
        query_node = None if code_type == "cfda" else self._find_node(codes["hierarchy"], query)
        if query_node and not (code_type == "psc" and query_node[0] in PSC_GROUPS):
            selected = [query_node]
        else:
            # Semantic search returns anything loosely related, so only keep results close to the best match
            min_score = 0.0
            margin = 0.0
            if is_semantic_search:
                best_score = max(
                    (self._get_best_score_in_tree(node) for node in codes["hierarchy"].values()), default=0
                )
                min_score = max(best_score * SEMANTIC_MIN_RELATIVE_SCORE, SEMANTIC_MIN_ABSOLUTE_SCORE)
                margin = SEMANTIC_DESCENDANT_MARGIN
                logger.info(
                    f"Semantic {code_type} search for '{query}': best score {best_score:.3f}, cutoff {min_score:.3f}"
                )
            selected = self._select_matched_nodes(codes["hierarchy"], code_type, min_score, margin)
            if is_semantic_search:
                selected = sorted(selected, key=lambda item: item[1]["score"], reverse=True)[:SEMANTIC_MAX_SELECTIONS]
                logger.info(
                    f"Selected {code_type} codes: {[(code, round(node['score'], 3)) for code, node in selected]}"
                )
        if len(selected) == 0:
            error_class, label = CODE_NOT_RECOGNIZED_ERRORS[code_type]
            raise error_class(f"{label}: {query}")

        if code_type == "cfda":
            programs = {
                code: SelectedCFDA(identifier=code, program_number=code, program_title=node["description"]).model_dump(
                    exclude_none=True
                )
                for code, node in selected
                if "." in code
            }
            return AIToolResult(results={"selectedCFDA": programs})

        values = [self._build_filter_value(code, code_type) for code, _ in selected]
        if code_type == "tas" and (is_semantic_search or self._is_tas_account_query(query)):
            # Periods of availability of one account are near duplicates, so select the account instead of each year
            values = [list(account) for account in dict.fromkeys(tuple(path[:2]) for path in values)]
        code_lists = CODE_LISTS_MODELS[code_type](**{inclusion: values})
        # 'counts' is calculated with the sync ORM when it's read, so leave it out of what is returned
        return AIToolResult(results={f"{code_type}Codes": code_lists.model_dump(exclude={"counts"})})

    @staticmethod
    def _normalize_query(query: str, code_type: str) -> str:
        """Reduce a query that wraps a code in extra words to the code itself, so it can be matched exactly: a leading
        label ("AL 14.257" -> "14.257"), or for assistance listings a program number inside a longer phrase."""
        normalized = query.strip()
        label_pattern = CODE_LABEL_PATTERNS.get(code_type)
        if label_pattern and (label := label_pattern.match(normalized)):
            normalized = normalized[label.end() :].strip()
        if code_type == "cfda":
            program_numbers = CFDA_PROGRAM_NUMBER_PATTERN.findall(normalized)
            if len(program_numbers) == 1:
                normalized = program_numbers[0]
        if normalized != query.strip():
            logger.info(f"Normalized {code_type} query '{query}' to '{normalized}'")
        return normalized

    @staticmethod
    async def validate_input(
        code_type: Literal["naics", "psc", "cfda", "tas"],
        inclusion: Literal["require", "exclude"] = "require",
    ) -> str | None:
        msg = None
        if code_type not in CODE_TYPE_CONFIGS:
            msg = f"Unsupported code type: {code_type}. Supported types: {list(CODE_TYPE_CONFIGS.keys())}"
        elif inclusion not in ["require", "exclude"]:
            msg = f"Unsupported code inclusion mode: {inclusion}. Supported types: [require, exclude]"
        elif code_type == "cfda" and inclusion == "exclude":
            msg = "Assistance listings can only be required, not excluded."

        return msg

    async def _handle_exact_search(
        self,
        query: str,
        config: CodeTypeConfig,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        budget_bureau_names: dict[str, str],
        top_k: int,
    ) -> dict[str, Any] | None:
        exact_qs, prefix_qs = self._query_exact_and_prefix_matches(config, query)

        # Exact and prefix code matches skip embeddings/fanout entirely
        if await exact_qs.aexists() or await prefix_qs.aexists():
            exact_results = await self._build_result_entries(
                exact_qs, config, code_type, score=1.0, budget_bureau_names=budget_bureau_names
            )
            prefix_results = await self._build_result_entries(
                prefix_qs, config, code_type, score=0.95, budget_bureau_names=budget_bureau_names
            )
            all_results = prefix_results | exact_results
            return await self._finalize_results(all_results, config, code_type, budget_bureau_names, top_k)
        else:
            return None

    async def _handle_hybrid_search(
        self,
        query: str,
        config: CodeTypeConfig,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        budget_bureau_names: dict[str, str],
        top_k: int,
        query_fanout: int | None = 3,
    ) -> dict[str, Any]:
        TEXT_WEIGHT: float = 0.5
        VECTOR_WEIGHT: float = 0.5

        model = config.model_class
        all_results = {}
        queries = [query]
        if bool(query_fanout) and not NUMERIC_CODE_QUERY_PATTERN.fullmatch(query):
            expansion_model = await AIModel.objects.aget(name="nova micro")
            queries = await expand_query(query, expansion_model, query_fanout)
            logger.info(f"Generated variations: {queries}")

        embedding_model = await AIModel.objects.aget(name="titan")
        for q in queries:
            logger.info(f"\nSearching for: '{q}'")
            try:
                embedding_generator = EmbeddingGenerator(model=embedding_model, dimensions=model.embedding_dimensions)
                embedding = await embedding_generator.agenerate_embedding(q)
                logger.info(f"Generated embedding: '{embedding[:3]}'")
            except Exception as e:
                logger.info(f"Embedding generation failed for '{q}': {e}")
                continue

            if not embedding:
                continue

            # Perform hybrid search
            qs = model.objects.filter(embedding__isnull=False)

            code_similarity = TrigramSimilarity(config.code_field, q)
            desc_similarity = TrigramSimilarity(config.description_field, q)
            text_score = Greatest(code_similarity, desc_similarity)

            qs = qs.annotate(
                vector_distance=CosineDistance("embedding", embedding),
                hybrid_score=(
                    (Value(TEXT_WEIGHT, output_field=FloatField()) * text_score)
                    + (Value(VECTOR_WEIGHT, output_field=FloatField()) * (1.0 - CosineDistance("embedding", embedding)))
                ),
            ).filter(vector_distance__lt=0.75)
            if code_type == "tas":
                qs = self._dedupe_tas_by_period_of_availability(qs)
            qs = qs.order_by("-hybrid_score")[:top_k]

            async for result in qs:
                code_value = getattr(result, config.code_field)

                # Keep best score if duplicate
                if code_value not in all_results or result.hybrid_score > all_results[code_value].score:
                    description_value = getattr(result, config.description_field, None)
                    entry = CodeResult(
                        code=code_value,
                        description=description_value,
                        score=result.hybrid_score,
                        matched_query=q,
                    )
                    if code_type == "tas" and hasattr(result, "budget_bureau_name"):
                        entry.budget_bureau_name = result.budget_bureau_name
                        parts = code_value.split("-")
                        if len(parts) >= 4:
                            aid = parts[-4]
                            main = parts[-2]
                            budget_bureau_names[f"{aid}-{main}"] = result.budget_bureau_name
                    all_results[code_value] = entry

        return await self._finalize_results(all_results, config, code_type, budget_bureau_names, top_k)

    @staticmethod
    def _query_exact_and_prefix_matches(config: CodeTypeConfig, query: str) -> tuple[QuerySet, QuerySet]:
        """Look up exact (case-insensitive) and prefix matches on the code field."""
        code_field = config.code_field
        model = config.model_class
        exact_qs = model.objects.filter(**{f"{code_field}__iexact": query})
        prefix_filter = Q(**{f"{code_field}__istartswith": query})

        # TAS rendering labels start with the agency, so an account code ("3200" or "014-0102") would never be a prefix
        # of one. Match those on the account's own fields instead.
        if model is TreasuryAppropriationAccount:
            if TAS_MAIN_ACCOUNT_PATTERN.fullmatch(query):
                prefix_filter |= Q(main_account_code=query)
            elif TAS_AID_MAIN_PATTERN.fullmatch(query):
                agency_id, main_account_code = query.split("-")
                prefix_filter |= Q(agency_id=agency_id, main_account_code=main_account_code)

        prefix_qs = model.objects.filter(prefix_filter).exclude(**{f"{code_field}__iexact": query})
        return exact_qs, prefix_qs

    @staticmethod
    def _dedupe_tas_by_period_of_availability(queryset: QuerySet) -> QuerySet:
        """Get a single tas symbol for each account with multiple time periods.  This prevents TASs that differ only
        in period of availability from filling the results window with near duplicates."""
        ranked = queryset.annotate(
            account_rank=Window(
                expression=RowNumber(),
                partition_by=[
                    F("allocation_transfer_agency_id"),
                    F("agency_id"),
                    F("main_account_code"),
                    F("sub_account_code"),
                ],
                order_by=F("hybrid_score").desc(),
            )
        )
        return ranked.filter(account_rank=1).order_by("-hybrid_score")

    @staticmethod
    async def _build_result_entries(
        queryset: QuerySet,
        config: CodeTypeConfig,
        code_type: str,
        score: float,
        budget_bureau_names: dict[str, str],
    ) -> dict[str, CodeResult]:
        entries = {}
        async for result in queryset:
            code_value = getattr(result, config.code_field)
            entry = CodeResult(
                code=code_value,
                description=getattr(result, config.description_field, None),
                score=score,
            )
            if code_type == "tas" and hasattr(result, "budget_bureau_name"):
                entry.budget_bureau_name = result.budget_bureau_name
                parts = code_value.split("-")
                if len(parts) >= 3:
                    aid_main = f"{parts[0]}-{parts[2]}"
                    budget_bureau_names[aid_main] = result.budget_bureau_name
            entries[code_value] = entry
        return entries

    async def _finalize_results(
        self,
        all_results: dict[str, CodeResult],
        config: CodeTypeConfig,
        code_type: str,
        budget_bureau_names: dict,
        top_k: int,
    ) -> dict[str, Any]:
        top_codes = [
            result.code for result in sorted(all_results.values(), key=lambda x: x.score, reverse=True)[:top_k]
        ]
        all_results = {code: result for code, result in all_results.items() if code in top_codes}
        ancestor_codes = set()
        for code in all_results.keys():
            ancestor_codes.update(config.get_all_ancestors(code))
        if ancestor_codes:
            if code_type == "tas":
                await self._add_tas_ancestors(ancestor_codes, all_results, budget_bureau_names)
            else:
                model = config.model_class
                ancestors = model.objects.filter(**{f"{config.code_field}__in": list(ancestor_codes)})
                async for ancestor in ancestors:
                    ancestor_code = getattr(ancestor, config.code_field)
                    if ancestor_code not in all_results:
                        all_results[ancestor_code] = CodeResult(
                            code=ancestor_code,
                            description=getattr(ancestor, config.description_field, None),
                        )
        root_codes = [code for code in all_results.keys() if not self._has_parent_in_results(code, all_results, config)]
        hierarchy = {}
        for root_code in root_codes:
            root_data = all_results[root_code]
            root_node = SearchResultNode(
                code=root_code,
                description=root_data.description,
                score=root_data.score,
                all_results=all_results,
                config=config,
                is_root=True,
            )
            hierarchy[root_code] = root_node.to_dict()
        if code_type == "psc":
            hierarchy = self._group_psc_roots_by_tier1(hierarchy)
        sorted_hierarchy = dict(
            sorted(hierarchy.items(), key=lambda x: self._get_best_score_in_tree(x[1]), reverse=True)
        )

        return {
            "hierarchy": sorted_hierarchy,
            "total_codes": sum(self._count_codes_in_tree(node) for node in sorted_hierarchy.values()),
        }

    @staticmethod
    def _group_psc_roots_by_tier1(hierarchy: dict) -> dict:
        """Wrap each PSC root ('A', 'D', '10', ...) under its synthetic Tier1 PSC_GROUPS label,
        matching the grouping the real PSC filter tree API uses (PSCFilterTree.toptier_search)."""
        rd_terms = PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms
        grouped: dict[str, dict] = {}
        for root_code, root_data in hierarchy.items():
            if root_code[0].isdigit():
                label = str(PscGroupLabels.PRODUCT)
            elif root_code[0] in rd_terms:
                label = str(PscGroupLabels.RESEARCH_AND_DEVELOPMENT)
            else:
                label = str(PscGroupLabels.SERVICE)

            bucket = grouped.setdefault(
                label,
                {
                    "code": label,
                    "description": "",
                    "score": 0.0,
                    "is_root": True,
                    "is_leaf": False,
                    "children": {},
                    "leaf_node_count": 0,
                },
            )
            if label == PscGroupLabels.RESEARCH_AND_DEVELOPMENT and root_code in rd_terms:
                # RnD is the only PSC where the root value is a valid description in our PSC table (where code = A).
                # Due to this we replace the label for RnD PSC on the root instead of creating a new root
                bucket["children"] = {**bucket["children"], **root_data["children"]}
                # The root's own node is dropped, so record whether the search matched it (and so the whole group)
                bucket["is_group_match"] = bucket.get("is_group_match", False) or root_data.get("score", 0.0) > 0
            else:
                root_data["is_root"] = False
                bucket["children"][root_code] = root_data
            bucket["score"] = max(bucket["score"], root_data.get("score", 0.0))
            bucket["leaf_node_count"] += root_data.pop("leaf_node_count", 0)
        return grouped

    @staticmethod
    async def _add_tas_ancestors(
        ancestor_codes: set[str], all_results: dict[str, CodeResult], budget_bureau_names: dict[str, str]
    ) -> None:
        """
        Special handling for TAS ancestors which come from multiple sources:
        - Top tier (3-digit): toptier_agency table
        - Middle tier (AID-MAIN): budget_bureau_name from original results (no DB record)
        - Full rendering label: TreasuryAppropriationAccount table
        """
        # Separate ancestor codes by type
        toptier_codes = set()
        aid_main_codes = set()
        for code in ancestor_codes:
            if "-" in code:
                aid_main_codes.add(code)
            else:
                toptier_codes.add(code)
        for aid_main, bureau_name in budget_bureau_names.items():
            if aid_main in aid_main_codes and aid_main not in all_results:
                all_results[aid_main] = CodeResult(code=aid_main, description=bureau_name)

        # Add top-tier ancestors from toptier_agency table
        if toptier_codes:
            toptier_agencies = ToptierAgency.objects.filter(toptier_code__in=list(toptier_codes))

            async for agency in toptier_agencies:
                if agency.toptier_code not in all_results:
                    # Use abbreviation if available, otherwise name
                    description = f"{agency.name} ({agency.abbreviation})"
                    all_results[agency.toptier_code] = CodeResult(code=agency.toptier_code, description=description)

    def _get_best_score_in_tree(self, node: dict) -> float:
        """Recursively find the best score in a tree"""
        best = node.get("score", 0)

        if "children" in node:
            for child in node["children"].values():
                child_best = self._get_best_score_in_tree(child)
                best = max(best, child_best)

        return best

    @staticmethod
    def _has_parent_in_results(code: str, all_results: dict[str, CodeResult], config: CodeTypeConfig) -> bool:
        """Check if code's parent exists in results"""
        if not config.get_parent_code:
            return False

        parent_code = config.get_parent_code(code)
        return parent_code and parent_code in all_results

    def _count_codes_in_tree(self, node: dict) -> int:
        """Recursively count codes in tree"""
        count = 1
        if "children" in node:
            for child in node["children"].values():
                count += self._count_codes_in_tree(child)
        return count

    def _find_node(self, hierarchy: dict, code: str) -> tuple[str, dict] | None:
        """Find the node for a code (case-insensitive) anywhere in the hierarchy, as a (code, node) pair."""
        for node_code, node in hierarchy.items():
            if node_code.lower() == code.lower():
                return node_code, node
            if found := self._find_node(node.get("children", {}), code):
                return found
        return None

    def _select_matched_nodes(
        self, hierarchy: dict, code_type: str, min_score: float = 0.0, margin: float = 0.0
    ) -> list[tuple[str, dict]]:
        """Pick the codes the search actually matched, as (code, node) pairs.

        The hierarchy also holds ancestors that were only added for context (they have a score of 0), and selecting
        one of those would select everything beneath it. PSC Tier1 group labels are skipped for the same reason, unless
        the search matched the group itself (the R&D "A" row, flagged with is_group_match), in which case the whole
        group is selected.

        A matched code covers all of the codes beneath it, so its descendants aren't selected separately. The
        exception is a descendant that scored higher than the code itself by more than the margin: the search found
        something clearly more specific, so that is selected instead of the broader code.
        """
        selected = []
        for code, node in hierarchy.items():
            children = node.get("children", {})
            # Group labels are only selectable when the search matched the group itself (the "A" row, for R&D)
            is_psc_group_label = code_type == "psc" and code in PSC_GROUPS and not node.get("is_group_match")
            is_match = node["score"] > 0 and node["score"] >= min_score and not is_psc_group_label
            best_descendant_score = max((self._get_best_score_in_tree(child) for child in children.values()), default=0)
            if is_match and best_descendant_score <= node["score"] + margin:
                selected.append((code, node))
            else:
                selected.extend(self._select_matched_nodes(children, code_type, min_score, margin))
        return selected

    @staticmethod
    def _is_tas_account_query(query: str) -> bool:
        """Whether a query is an account code (a main account like "3200" or AID-MAIN like "014-0102") rather than a
        TAS rendering label."""
        return bool(TAS_MAIN_ACCOUNT_PATTERN.fullmatch(query) or TAS_AID_MAIN_PATTERN.fullmatch(query))

    @staticmethod
    def _build_filter_value(code: str, code_type: str) -> str | list[str]:
        """Format a code the way the filter's 'require'/'exclude' expect it: a flat code for NAICS and a path from the
        root node down for PSC and TAS."""
        if code_type == "naics":
            return code
        return build_tas_path(code) if code_type == "tas" else build_psc_path(code)


lookup_code_tool = AITool(
    function=CodeLookupTool().lookup_codes,
    logging=lambda tool_input: f"Searching {tool_input['code_type'].upper()} codes for '{tool_input['query']}'",
    description=AIToolDescription(
        name="lookup_codes",
        description="""
            Look up codes across several federal reference code systems using a mix of exact
            matching, prefix matching, description text search, and semantic similarity search.

            Supported code_type values:
            - naics: North American Industry Classification System codes
            - psc: Product and Service Codes
            - cfda: Catalog of Federal Domestic Assistance program numbers
            - tas: Treasury Account Symbol components

            Matching behavior (in priority order):
            1. Exact match: if the query exactly matches a code (case-insensitive), only that
               code (plus its ancestors) is returned immediately, skipping semantic search.
            2. Prefix match: if the query is a prefix of one or more codes, those codes are
               returned.
            3. Hybrid search: otherwise, the query is matched against both codes and
               descriptions using trigram text similarity, combined with semantic similarity
               from generated text embeddings. The query may also be automatically expanded into
               related variations to broaden semantic recall.

            The matched codes are returned ready to use in the execute_filter tool, under the key for the
            code_type (naicsCodes, pscCodes, tasCodes, or selectedCFDA). For naics, psc, and tas the codes are
            placed in "require" or "exclude" based on the inclusion value. Pass the returned value to
            execute_filter unchanged; do not edit the codes or add counts, which are calculated for you. Only
            codes that matched the query are returned. A code that covers others (for example the NAICS sector
            "23") selects everything beneath it. If nothing matches, an error is returned: retry with a
            different or simpler query, or continue without the filter if it isn't essential.

            When to use this tool:
            - Use it whenever the request involves a NAICS code or industry, a PSC code or the products or
              services being purchased, a Treasury Account Symbol or federal account, or an assistance listing
              (CFDA) program for grants and other assistance programs by subject. Look codes up instead of
              writing them from memory.
            - Do not use it for DEFC or disaster codes, dollar amounts, dates, locations, agencies, recipients,
              award types, or set-asides. Those have their own tools or filters.
        """,
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Code or description to search for"},
                "code_type": {
                    "type": "string",
                    "enum": ["naics", "psc", "cfda", "tas"],
                    "description": "Type of code to search for",
                },
                "inclusion": {
                    "type": "string",
                    "enum": ["require", "exclude"],
                    "description": "Optional. Whether the generated codes should be required or blocked from results",
                    "default": "require",
                },
                "top_k": {"type": "integer", "description": "Number of results to return (default: 20)", "default": 20},
            },
            "required": ["query", "code_type"],
        },
    ),
)
