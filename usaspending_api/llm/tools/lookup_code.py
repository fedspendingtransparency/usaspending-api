import logging
from typing import Any, Literal, Callable
from dataclasses import dataclass

from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import Q, Value, FloatField
from django.db.models.functions import Greatest

from pgvector.django import CosineDistance

from usaspending_api.llm.models.py_models import AIToolDescription, AITool
from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.llm.tools.expand_query import expand_query
from usaspending_api.references.models.naics import NAICS
from usaspending_api.references.models.cfda import Cfda
from usaspending_api.references.models.psc import PSC
from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount
from usaspending_api.llm.tools.helpers import hierarchy_builders
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


@dataclass
class CodeTypeConfig:
    """Configuration for each code type"""

    model_class: type
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
        get_parent_code=hierarchy_builders.get_naics_parent,
        get_all_ancestors=hierarchy_builders.get_naics_ancestors,
    ),
    "psc": CodeTypeConfig(
        model_class=PSC,
        code_field="code",
        description_field="description",
        get_parent_code=hierarchy_builders.get_psc_parent,
        get_all_ancestors=hierarchy_builders.get_psc_ancestors,
    ),
    "cfda": CodeTypeConfig(
        model_class=Cfda,
        code_field="program_number",
        description_field="program_title",
        get_parent_code=hierarchy_builders.get_cfda_parent,
        get_all_ancestors=hierarchy_builders.get_cfda_ancestors,
    ),
    "tas": CodeTypeConfig(
        model_class=TreasuryAppropriationAccount,
        code_field="tas_rendering_label",
        description_field="account_title",
        get_parent_code=hierarchy_builders.get_tas_parent,
        get_all_ancestors=hierarchy_builders.get_tas_ancestors,
    ),
}


class SearchResultNode:
    """Node for search results hierarchy"""

    def __init__(
        self,
        code: str,
        description: str,
        score: float,
        all_results: dict[str, dict],  # Map of code -> result data
        config: CodeTypeConfig,
    ):
        self.code = code
        self.description = description
        self.score = score
        self.children: list[SearchResultNode] = []
        self.config = config

        # Build children
        self._populate_children(all_results)

    def _populate_children(self, all_results: dict[str, dict]) -> None:
        """Build children from results that extend this code"""

        for child_code, child_data in all_results.items():
            if self._is_direct_child(child_code):
                child_node = SearchResultNode(
                    code=child_code,
                    description=child_data["description"],
                    score=child_data["score"],
                    all_results=all_results,
                    config=self.config,
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
            "children": {
                child.code: child.to_dict() for child in sorted(self.children, key=lambda x: x.score, reverse=True)
            },
        }


class CodeLookupTool:
    """Generalized tool for looking up various code types using hybrid text + vector similarity search"""

    def lookup_codes(
        self,
        query: str,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        text_weight: float = 0.4,
        vector_weight: float = 0.6,
        top_k: int = 20,
        use_fanout: bool = True,
        num_variations: int = 3,
    ) -> dict[str, Any]:
        """
        Hybrid search for various code types combining text matching and vector similarity.
        """
        if code_type not in CODE_TYPE_CONFIGS:
            return {
                "error": f"Unsupported code type: {code_type}. Supported types: {list(CODE_TYPE_CONFIGS.keys())}",
                "results": [],
            }

        config = CODE_TYPE_CONFIGS[code_type]
        model = config.model_class

        queries = [query]
        if use_fanout:
            print(f"Expanding query: '{query}'")
            queries = expand_query(query, num_variations)
            print(f"Generated variations: {queries}")

        all_results = {}  # Use dict to deduplicate by code

        for q in queries:
            print(f"\nSearching for: '{q}'")

            # Generate embedding for this variation
            embedding = None
            try:
                embedding_generator = EmbeddingGenerator(dimensions=model.embedding_dimensions)
                embedding = embedding_generator.generate_embedding(q)
                print(f"Generated embedding: '{embedding[:3]}'")
            except Exception as e:
                print(f"Embedding generation failed for '{q}': {e}")
                continue

            if not embedding:
                continue

            # Perform hybrid search
            qs = model.objects.filter(embedding__isnull=False)
            print(f"number of models={qs.count()}")

            code_similarity = TrigramSimilarity(config.code_field, q)
            desc_similarity = TrigramSimilarity(config.description_field, q)
            text_score = Greatest(code_similarity, desc_similarity)

            qs = (
                qs.annotate(
                    vector_distance=CosineDistance("embedding", embedding),
                    text_similarity=text_score,
                    hybrid_score=Value(text_weight, output_field=FloatField()) * text_score
                    + Value(vector_weight, output_field=FloatField()) * (1.0 - CosineDistance("embedding", embedding)),
                )
                .filter(vector_distance__lt=0.75)
                .order_by("-hybrid_score")[:top_k]
            )

            # Add results to collection
            for result in qs:
                code_value = getattr(result, config.code_field)

                # Keep best score if duplicate
                if code_value not in all_results or result.hybrid_score > all_results[code_value]["score"]:
                    description_value = getattr(result, config.description_field, None)
                    all_results[code_value] = {
                        "code": code_value,
                        "description": description_value,
                        "score": result.hybrid_score,
                        "matched_query": q,
                    }
                    if code_type == "tas" and hasattr(result, "budget_bureau_name"):
                        all_results[code_value]["budget_bureau_name"] = result.budget_bureau_name

        # Sort by score and take top_k
        results = sorted(all_results.values(), key=lambda x: x["score"], reverse=True)[:top_k]

        for item in results:
            print(f"  {item['code']} - {item['description']}: {item['score']:.3f} (matched: '{item['matched_query']}')")

        print(f"\nReturning {len(results)} unique codes from {len(queries)} query variations")
        return {"results": results, "count": len(results)}

    def lookup_codes_hierarchical(
        self,
        query: str,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        top_k: int = 20,
    ) -> dict[str, Any]:
        """Search with hierarchy awareness using existing tree-building pattern"""

        config = CODE_TYPE_CONFIGS[code_type]

        results = self.lookup_codes(query, code_type, top_k=top_k)
        results_by_code = {}
        budget_bureau_names = {}
        for r in results["results"]:
            # Build map of all results by code
            results_by_code[r["code"]] = {
                "code": r["code"],
                "description": r.get("description"),
                "score": r.get("score", 0),
            }

            if code_type == "tas" and r.get("budget_bureau_name"):
                # Extract AID-MAIN from full rendering label
                parts = r["code"].split("-")
                if len(parts) >= 3:
                    aid_main = f"{parts[0]}-{parts[2]}"
                    budget_bureau_names[aid_main] = r["budget_bureau_name"]

        # Add ancestor codes to results
        ancestor_codes = set()
        for code in results_by_code.keys():
            if config.get_all_ancestors:
                ancestors = config.get_all_ancestors(code)
                ancestor_codes.update(ancestors)

        # Fetch ancestors from database
        if ancestor_codes:
            if code_type == "tas":
                self._add_tas_ancestors(ancestor_codes, results_by_code, budget_bureau_names, config)
            else:
                Model = config.model_class
                ancestors = Model.objects.filter(**{f"{config.code_field}__in": list(ancestor_codes)})

                for ancestor in ancestors:
                    ancestor_code = getattr(ancestor, config.code_field)
                    if ancestor_code not in results_by_code:
                        results_by_code[ancestor_code] = {
                            "code": ancestor_code,
                            "description": getattr(ancestor, config.description_field, None),
                            "score": 0,
                        }

        # Find root nodes (codes with no parent in results)
        root_codes = [
            code for code in results_by_code.keys() if not self._has_parent_in_results(code, results_by_code, config)
        ]

        # Build tree from roots
        hierarchy = {}

        for root_code in root_codes:
            root_data = results_by_code[root_code]
            root_node = SearchResultNode(
                code=root_code,
                description=root_data["description"],
                score=root_data["score"],
                all_results=results_by_code,
                config=config,
            )
            hierarchy[root_code] = root_node.to_dict()
        # Sort roots by score
        sorted_hierarchy = dict(
            sorted(hierarchy.items(), key=lambda x: self._get_best_score_in_tree(x[1]), reverse=True)
        )

        return {
            "hierarchy": sorted_hierarchy,
            "total_codes": sum(self._count_codes_in_tree(node) for node in sorted_hierarchy.values()),
        }

    def _add_tas_ancestors(
        self, ancestor_codes: set[str], results_by_code: dict, budget_bureau_names: dict, config: CodeTypeConfig
    ) -> None:
        """
        Special handling for TAS ancestors which come from multiple sources:
        - Top tier (3-digit): toptier_agency table
        - Middle tier (AID-MAIN): budget_bureau_name from original results (no DB record)
        - Full rendering label: TreasuryAppropriationAccount table
        """

        # Separate ancestor codes by type
        toptier_codes = set()  # 3-digit codes
        aid_main_codes = set()  # AID-MAIN format

        for code in ancestor_codes:
            if "-" in code:
                # This is AID-MAIN format
                aid_main_codes.add(code)
            else:
                # This is a toptier code (3 digits)
                toptier_codes.add(code)

        # Add middle-tier ancestors (AID-MAIN) from budget_bureau_names

        for aid_main, bureau_name in budget_bureau_names.items():
            if aid_main in aid_main_codes and aid_main not in results_by_code:
                results_by_code[aid_main] = {
                    "code": aid_main,
                    "description": bureau_name,
                    "score": 0,
                    "display": None,
                }

        # Add top-tier ancestors from toptier_agency table
        if toptier_codes:
            toptier_agencies = ToptierAgency.objects.filter(toptier_code__in=list(toptier_codes))

            for agency in toptier_agencies:
                if agency.toptier_code not in results_by_code:
                    # Use abbreviation if available, otherwise name
                    description = f"{agency.name} ({agency.abbreviation})"
                    results_by_code[agency.toptier_code] = {
                        "code": agency.toptier_code,
                        "description": description,
                        "score": 0,
                        "display": None,
                    }

    def _get_best_score_in_tree(self, node: dict) -> float:
        """Recursively find the best score in a tree"""
        best = node.get("score", 0)

        if "children" in node:
            for child in node["children"].values():
                child_best = self._get_best_score_in_tree(child)
                best = max(best, child_best)

        return best

    def _has_parent_in_results(self, code: str, results_by_code: dict, config: CodeTypeConfig) -> bool:
        """Check if code's parent exists in results (adapted from BaseHierarchicalFilter)"""
        if not config.get_parent_code:
            return False

        parent_code = config.get_parent_code(code)
        return parent_code and parent_code in results_by_code

    def _code_is_descendant(self, code: str, ancestor_code: str, config: CodeTypeConfig) -> bool:
        """Check if code is descendant of ancestor (adapted from code_is_parent_of)"""
        if code == ancestor_code:
            return False

        if config.get_all_ancestors:
            ancestors = config.get_all_ancestors(code)
            return ancestor_code in ancestors

        return False

    def _count_codes_in_tree(self, node: dict) -> int:
        """Recursively count codes in tree"""
        count = 1
        if "children" in node:
            for child in node["children"].values():
                count += self._count_codes_in_tree(child)
        return count


lookup_codes_tool = AITool(
    function=CodeLookupTool().lookup_codes,
    logging=lambda tool_input: f"🔍 searching {tool_input['code_type'].upper()} index for '{tool_input['query']}'",
    description=AIToolDescription(
        name="lookup_codes",
        description="""
            Search for various code types using hybrid text + semantic similarity search.
            Automatically generates embeddings from your query for semantic understanding.
            Supported code types:
            - naics: North American Industry Classification System codes
            - psc: Product and Service Codes
            - cfda: Catalog of Federal Domestic Assistance numbers
            - tas: Treasury Account Symbol components
            The tool combines:
            - Exact/prefix matching on codes
            - Text matching on descriptions
            - Semantic similarity using AI-generated embeddings
            Examples:
            - lookup_codes('construction', 'naics') → Construction industry codes
            - lookup_codes('541330', 'naics') → Exact NAICS code
            - lookup_codes('software', 'psc') → Software-related PSC codes
            - lookup_codes('10.557', 'cfda') → Specific CFDA program
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
                "top_k": {"type": "integer", "description": "Number of results to return (default: 20)", "default": 20},
            },
            "required": ["query", "code_type"],
        },
    ),
)
