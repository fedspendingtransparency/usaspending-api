import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Literal

from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import F, FloatField, Model, Q, QuerySet, Value, Window
from django.db.models.functions import Greatest, Length, RowNumber
from pgvector.django import CosineDistance

from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import AITool, AIToolDescription, AIToolResult, SelectedCFDA
from usaspending_api.llm.tools.exceptions import (
    AssistanceListingNotRecognizedError,
    InvalidToolInputError,
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

CODE_NOT_FOUND_INSTRUCTIONS = (
    "Try a shorter or more general query (e.g. a top-level category, a partial code, or a single keyword instead of a "
    "full phrase), or try a different code_type if the query may describe a different kind of code. If no reasonable "
    "match exists, omit this filter."
)
# A query that isn't an assistance listing number is usually a mis-formatted code or a program name
CFDA_NOT_FOUND_INSTRUCTIONS = (
    "Do not retry the same string. If the query was an assistance listing number, strip any prefix such as 'AL' or "
    "'CFDA' and search with just the number, or search by the program name instead. Retry at most once; if there is "
    "still no match, omit this filter."
)
# TAS lookups are often attempted for values that are not treasury accounts at all (dates, fiscal years, topics)
TAS_NOT_FOUND_INSTRUCTIONS = (
    "Treasury accounts are identified by agency, account number, or account title. Do not use this tool for dates, "
    "fiscal years, time periods, or general topics; those belong to other filters. Do not retry the same string. "
    "If the value is not a treasury account, omit this filter and continue."
)
NOT_FOUND_INSTRUCTIONS = {"cfda": CFDA_NOT_FOUND_INSTRUCTIONS, "tas": TAS_NOT_FOUND_INSTRUCTIONS}

# Semantic search always returns its nearest neighbors, however loosely related. A match must score at least this much,
# so a query with nothing related returns an error instead of its nearest neighbors. Choosing among the codes that
# pass is left to the llm, which sees their scores.
SEMANTIC_MIN_SCORE = 0.3

# Where the llm puts the codes it chooses, by code type. {inclusion} is 'require' or 'exclude'.
FILTER_DESTINATIONS = {
    "naics": "naicsCodes.{inclusion} (a list of the filter_value strings)",
    "psc": "pscCodes.{inclusion} (a list of the filter_value paths)",
    "tas": "tasCodes.{inclusion} (a list of the filter_value paths)",
    "cfda": "selectedCFDA (an object keyed by program number, with the filter_value as each value)",
}
# Children shown beneath a code. A code with many near duplicates beneath it (e.g. the variants of a PSC category) is
# summarized by its own description and covers count instead of listing all of them.
MAX_CHILDREN_SHOWN = 5
SELECTION_INSTRUCTIONS = (
    "Choose the code or codes in the tree that best represent what the user asked for. Judge each candidate by its "
    "description, its place in the tree, and its score together. A score is search similarity, not correctness, so a "
    "lower scoring code can be the better fit. A code with matched=false was only added to show where its children "
    "sit; it can still be the best choice if the user means its whole category. Choosing a code chooses everything "
    "beneath it (see covers). When the user names a category, industry, or topic, choose the code whose description "
    "is that category, even if codes beneath it score higher; those are only parts of it. Choose a code beneath it "
    "only when the user asked for that specific thing, or when the category would include things the user did not "
    "ask for. When a code and a code beneath it cover the same number of codes, choose the higher one. Only the "
    "best scoring codes beneath a code are shown; more_children counts the others, and choosing the code still "
    "covers them. Do not choose codes that are only loosely related to the request; if none of them fit, leave this "
    "filter out. Copy the filter_value of each code you choose into {destination}, and combine the codes from "
    "separate lookups. "
)
EXACT_MATCH_INSTRUCTIONS = (
    "The query is itself a code, so it is the only code matched. Use it unless the user meant something else. Copy "
    "its filter_value into {destination}. "
)
CODE_TYPE_INSTRUCTIONS = {
    "tas": (
        "Prefer an account (the AID-MAIN level) to one of its periods of availability unless the user named a "
        "period, and choose an agency only when the user means all of its accounts."
    ),
}

# Queries that are a TAS main account ("3200") or AID-MAIN ("014-0102") rather than the start of a rendering label
TAS_MAIN_ACCOUNT_PATTERN = re.compile(r"\d{4}")
TAS_AID_MAIN_PATTERN = re.compile(r"\d{3}-\d{4}")
# What a code scores when it is the query, or the query is the start of it
EXACT_MATCH_SCORE = 1.0
PREFIX_MATCH_SCORE = 0.95
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

        Returns the matched codes with their ancestors as a tree, so the llm can choose the codes that best fit the
        request using their descriptions, scores, and place in the hierarchy.
        """

        if msg := await self.validate_input(code_type, inclusion):
            raise InvalidToolInputError(msg)

        config = CODE_TYPE_CONFIGS[code_type]
        budget_bureau_names = {}
        query = self._normalize_query(query, code_type)

        codes = await self._handle_exact_search(query, config, code_type, budget_bureau_names, top_k)
        if codes is None:
            codes = await self._handle_hybrid_search(query, config, code_type, budget_bureau_names, top_k, query_fanout)
        hierarchy = codes["hierarchy"]
        if code_type == "tas" and self._is_tas_account_query(query):
            hierarchy = self._mark_tas_accounts_matched(hierarchy)

        # A query that is itself a code in the hierarchy (e.g. the TAS agency "309") matches that code alone, even
        # when the node is only an ancestor of what matched. CFDA is excluded as only its programs can be chosen.
        query_node = None if code_type == "cfda" else self._find_node(hierarchy, query)
        is_exact_code = bool(query_node) and not (code_type == "psc" and query_node[0] in PSC_GROUPS)
        if is_exact_code:
            hierarchy = self._keep_only_path_to(hierarchy, query_node[0])
        if not hierarchy:
            error_class, label = CODE_NOT_RECOGNIZED_ERRORS[code_type]
            raise error_class(
                f"{label}: {query}",
                instructions=NOT_FOUND_INSTRUCTIONS.get(code_type, CODE_NOT_FOUND_INSTRUCTIONS),
            )

        covered_counts = await self._count_covered_codes(hierarchy, code_type)
        tree = self._order_output_nodes(self._rank_output_nodes(hierarchy, code_type, covered_counts))
        logger.info(f"Returning {code_type} tree for '{query}' with {codes['total_codes']} codes")

        return AIToolResult(
            results={"code_type": code_type, "inclusion": inclusion, "query": query, "tree": tree},
            instructions=self._build_instructions(code_type, inclusion, is_exact_code),
        )

    @staticmethod
    def _build_instructions(code_type: str, inclusion: str, is_exact_code: bool) -> str:
        """How the llm should choose among the codes returned and where to put the ones it chooses."""
        template = EXACT_MATCH_INSTRUCTIONS if is_exact_code else SELECTION_INSTRUCTIONS
        destination = FILTER_DESTINATIONS[code_type].format(inclusion=inclusion)
        return template.format(destination=destination) + CODE_TYPE_INSTRUCTIONS.get(code_type, "")

    def _keep_only_path_to(self, hierarchy: dict, code: str) -> dict:
        """Reduce a hierarchy to the nodes from a root down to a code, dropping everything beneath that code. The code
        was asked for by name, so it counts as an exact match even when it was only in the hierarchy as an ancestor
        (e.g. a TAS agency)."""
        for node_code, node in hierarchy.items():
            if node_code.lower() == code.lower():
                return {node_code: {**node, "score": node["score"] or 1.0, "children": {}}}
            if children := self._keep_only_path_to(node.get("children", {}), code):
                return {node_code: {**node, "children": children}}
        return {}

    def _rank_output_nodes(
        self, nodes: dict, code_type: str, covered_counts: dict[str, int]
    ) -> list[tuple[float, dict]]:
        """Format sibling hierarchy nodes for the llm, each with the best score found beneath it.

        A PSC Tier1 group ('Service', 'Product', 'Research and Development') is only a label unless the search matched
        the group itself (the R&D "A" row), so an unmatched group is left out and its children take its place. The
        filter_value of each code already starts with its group."""
        ranked = []
        for code, node in nodes.items():
            if code_type == "psc" and code in PSC_GROUPS and not node.get("is_group_match"):
                ranked.extend(self._rank_output_nodes(node.get("children", {}), code_type, covered_counts))
            else:
                output = self._build_output_node(code, node, code_type, covered_counts)
                ranked.append((self._get_best_score_in_tree(node), output))
        return ranked

    def _build_output_node(self, code: str, node: dict, code_type: str, covered_counts: dict[str, int]) -> dict:
        """Format a hierarchy node for the llm: what the code is, how well it matched, and the value to use for it in
        execute_filter. Only the best scoring children are shown; more_children counts the rest, which are still
        covered when the node is chosen."""
        output = {
            "code": code,
            "description": node["description"],
            "score": round(node["score"], 2),
            "matched": node["score"] > 0,
        }
        if code in covered_counts:
            output["covers"] = covered_counts[code]
        # An assistance listing agency is not a program, so it can't be filtered on
        if code_type != "cfda" or "." in code:
            output["filter_value"] = self._build_filter_value(code, code_type, node["description"])

        ranked_children = self._rank_output_nodes(node.get("children", {}), code_type, covered_counts)
        children = self._order_output_nodes(ranked_children)
        if children:
            output["children"] = children[:MAX_CHILDREN_SHOWN]
            if len(children) > MAX_CHILDREN_SHOWN:
                output["more_children"] = len(children) - MAX_CHILDREN_SHOWN
        return output

    @staticmethod
    def _order_output_nodes(ranked: list[tuple[float, dict]]) -> list[dict]:
        """Order formatted nodes by the best score beneath each, highest first."""
        return [output for _, output in sorted(ranked, key=lambda item: item[0], reverse=True)]

    @staticmethod
    async def _count_covered_codes(hierarchy: dict, code_type: str) -> dict[str, int]:
        """Count the lowest level codes (6 digit NAICS, 4 character PSC) under each code in the tree, which is how much
        of the filter choosing it covers."""
        if code_type not in ("naics", "psc"):
            return {}

        def tree_codes(nodes: dict) -> list[str]:
            codes = []
            for code, node in nodes.items():
                # PSC Tier1 groups are labels, not codes
                if not (code_type == "psc" and code in PSC_GROUPS):
                    codes.append(code)
                codes.extend(tree_codes(node.get("children", {})))
            return codes

        codes = tree_codes(hierarchy)
        if not codes:
            return {}
        model = CODE_TYPE_CONFIGS[code_type].model_class
        leaves = model.objects.annotate(code_length=Length("code")).filter(code_length=6 if code_type == "naics" else 4)
        leaves = leaves.filter(code__regex=r"^(" + "|".join(re.escape(code) for code in codes) + ")")
        leaf_codes = [leaf async for leaf in leaves.values_list("code", flat=True)]
        return {code: sum(leaf.startswith(code) for leaf in leaf_codes) for code in codes}

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
                exact_qs, config, code_type, score=EXACT_MATCH_SCORE, budget_bureau_names=budget_bureau_names
            )
            prefix_results = await self._build_result_entries(
                prefix_qs, config, code_type, score=PREFIX_MATCH_SCORE, budget_bureau_names=budget_bureau_names
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
                if result.hybrid_score < SEMANTIC_MIN_SCORE:
                    continue
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
                # With no bureau to describe the account, use the title of its best matching TAS
                description = bureau_name or CodeLookupTool._best_child_description(aid_main, all_results)
                all_results[aid_main] = CodeResult(code=aid_main, description=description)

        # Add top-tier ancestors from toptier_agency table
        if toptier_codes:
            toptier_agencies = ToptierAgency.objects.filter(toptier_code__in=list(toptier_codes))

            async for agency in toptier_agencies:
                if agency.toptier_code not in all_results:
                    # Use abbreviation if available, otherwise name
                    description = f"{agency.name} ({agency.abbreviation})"
                    all_results[agency.toptier_code] = CodeResult(code=agency.toptier_code, description=description)

    @staticmethod
    def _best_child_description(code: str, all_results: dict[str, CodeResult]) -> str | None:
        """The description of the best scoring result directly beneath a code."""
        children = [
            result
            for result in all_results.values()
            if result.description and hierarchy_parsers.get_tas_parent(result.code) == code
        ]
        return max(children, key=lambda result: result.score).description if children else None

    @staticmethod
    def _is_tas_account_query(query: str) -> bool:
        """Whether a query is an account code (a main account like "3200" or AID-MAIN like "014-0102") rather than a
        TAS rendering label."""
        return bool(TAS_MAIN_ACCOUNT_PATTERN.fullmatch(query) or TAS_AID_MAIN_PATTERN.fullmatch(query))

    def _mark_tas_accounts_matched(self, hierarchy: dict) -> dict:
        """An account code query matches the accounts themselves. Their AID-MAIN nodes are only in the hierarchy as
        ancestors of the matching TAS, with no score, which makes the periods of availability beneath them look like
        the match instead of the account."""
        marked = {}
        for code, node in hierarchy.items():
            is_unscored_account = bool(TAS_AID_MAIN_PATTERN.fullmatch(code)) and not node["score"]
            marked[code] = {
                **node,
                "score": PREFIX_MATCH_SCORE if is_unscored_account else node["score"],
                "children": self._mark_tas_accounts_matched(node.get("children", {})),
            }
        return marked

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

    @staticmethod
    def _build_filter_value(code: str, code_type: str, description: str | None = None) -> str | list[str] | dict:
        """Format a code the way execute_filter expects it: a flat code for NAICS, a path from the root node down for
        PSC and TAS, and the program object for an assistance listing."""
        if code_type == "naics":
            return code
        if code_type == "cfda":
            return SelectedCFDA(identifier=code, program_number=code, program_title=description).model_dump(
                exclude_none=True
            )
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
            - tas: Treasury accounts, identified by agency code, main account number, or account title

            Never use this tool for dates, fiscal years (e.g. "FY25"), quarters, or other time periods; those are
            handled by the time period filters.

            Matching behavior (in priority order):
            1. Exact match: if the query exactly matches a code (case-insensitive), only that
               code (plus its ancestors) is returned immediately, skipping semantic search.
            2. Prefix match: if the query is a prefix of one or more codes, those codes are
               returned.
            3. Hybrid search: otherwise, the query is matched against both codes and
               descriptions using trigram text similarity, combined with semantic similarity
               from generated text embeddings. The query may also be automatically expanded into
               related variations to broaden semantic recall.

            The result is a tree of the codes that matched, along with the ancestors that show where they sit in
            the hierarchy. Each code has its description, a score for how well it matched the query, whether it
            matched or is only context, how many lowest level codes it covers, and a filter_value to use in
            execute_filter. Only the best scoring children of a code are listed. You choose which codes to use, so
            read the instructions returned with the tree: they explain how to choose and where the filter_value of
            each code goes.
            Choosing a code includes everything beneath it. The tree can contain codes that are only loosely
            related, so do not choose a code just because it is listed. If nothing matches, an error is returned
            with instructions: follow them, do not repeat the same query, and continue without the filter if it
            isn't essential.

            When to use this tool:
            - Use it whenever the request involves a NAICS code or industry, a PSC code or the products or
              services being purchased, a Treasury Account Symbol or federal account, or an assistance listing
              (CFDA) program for grants and other assistance programs by subject. Look codes up instead of
              writing them from memory.
            - Do not use it for DEFC or disaster codes, dollar amounts, dates or fiscal years, locations,
              agencies, recipients, award types, or set-asides. Those have their own tools or filters.
            - Words that are part of a company, organization, or person's name are never an industry, product, or
              program.

            Choosing between naics and psc:
            - naics describes the industry the recipient business operates in (for example "companies in the mining
              industry").
            - psc describes what the government is buying: a product, a service, or research and development
              (for example "contracts for research and development" or "contracts for weapons").
            - If the request is for the work or items being purchased rather than the kind of business receiving the
              award, use psc. Use both only when the user asks about both.
        """,
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A code, or a description of what the code covers. Not a date or time period.",
                },
                "code_type": {
                    "type": "string",
                    "enum": ["naics", "psc", "cfda", "tas"],
                    "description": (
                        "Type of code to search for. Use naics for the industry of the recipient business and psc for "
                        "the product, service, or research being purchased. Use tas only when the user names a "
                        "Treasury account, federal account, or account title; never for dates, fiscal years, or "
                        "time periods."
                    ),
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
