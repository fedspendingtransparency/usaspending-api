import json
from typing import Any, Literal
from dataclasses import dataclass

import boto3
from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import Q, Value, FloatField
from django.db.models.functions import Greatest

from pgvector.django import CosineDistance

from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import AIToolDescription, AITool
from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator
from usaspending_api.references.models.naics import NAICS
from usaspending_api.references.models.cfda import Cfda
from usaspending_api.references.models.psc import PSC
from usaspending_api.accounts.models.treasury_appropriation_account import TreasuryAppropriationAccount


@dataclass
class CodeTypeConfig:
    """Configuration for each code type"""

    model_class: type
    code_field: str
    description_field: str
    entity_name: str
    embedding_dimensions: int = 256


# Configuration for each supported code type
CODE_TYPE_CONFIGS = {
    "naics": CodeTypeConfig(
        model_class=NAICS,
        code_field="code",
        description_field="description",
        entity_name="NAICS",
        embedding_dimensions=256,
    ),
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
        Model = config.model_class

        queries = [query]
        if use_fanout:
            print(f"Expanding query: '{query}'")
            queries = self.expand_query(query, num_variations)
            print(f"Generated variations: {queries}")

        all_results = {}  # Use dict to deduplicate by code

        for q in queries:
            print(f"\nSearching for: '{q}'")

            # Generate embedding for this variation
            embedding = None
            try:
                embedding_generator = EmbeddingGenerator(dimensions=config.embedding_dimensions)
                embedding = embedding_generator.generate_embedding(q)
            except Exception as e:
                print(f"Embedding generation failed for '{q}': {e}")
                continue

            if not embedding:
                continue

            # Perform hybrid search
            qs = Model.objects.filter(embedding__isnull=False)

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
            import ipdb

            ipdb.set_trace()

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

        # Sort by score and take top_k
        sorted_results = sorted(all_results.values(), key=lambda x: x["score"], reverse=True)[:top_k]

        # Transform to output format
        results = []
        for item in sorted_results:
            print(f"  {item['code']} - {item['description']}: {item['score']:.3f} (matched: '{item['matched_query']}')")
            results.append(self._transform_to_selected_code(item["code"], item["description"], item["score"], config))

        print(f"\nReturning {len(results)} unique codes from {len(queries)} query variations")
        return {"results": results, "count": len(results)}

    def _transform_to_selected_code(
        self, code: str, description: str | None, score: int | None, config: CodeTypeConfig
    ) -> dict[str, Any]:
        """Transform OpenSearch result to Selected code format"""

        identifier = code

        filter_obj = {"require": [code]}

        # Build title with description if available
        if description and description != code:
            title = f"{code} - {description}"
        else:
            title = code

        display_obj = {"entity": config.entity_name, "standalone": code, "title": title}

        return {"identifier": identifier, "filter": filter_obj, "display": display_obj, "score": score}

    def expand_query(self, query: str, num_variations: int = 3) -> list[str]:
        """Generate related search queries using Amazon Bedrock converse API with tool"""

        try:
            client = boto3.client(service_name="bedrock-runtime")

            # Define the tool for query expansion
            tool_spec = {
                "toolSpec": {
                    "name": "expand_search_query",
                    "description": "Generates related search phrases to improve search recall",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "variations": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": f"List of {num_variations} related search phrases",
                                }
                            },
                            "required": ["variations"],
                        }
                    },
                }
            }

            tool_config = {"tools": [tool_spec]}

            system = [
                {
                    "text": f"""You are a search query expansion assistant. Given a search query, generate {num_variations} related search phrases that would help find similar items.
                
                    Examples:
                    - Query: "aerospace" → ["aircraft manufacturing", "space technology", "aviation industry"]
                    - Query: "software development" → ["application programming", "IT services", "custom software"]
                    - Query: "construction" → ["building contractors", "infrastructure development", "civil engineering"]
                
                    Generate variations that are:
                    1. Semantically related to the original query
                    2. Use different terminology or synonyms
                    3. Cover different aspects of the topic"""
                }
            ]

            messages = [
                {
                    "role": "user",
                    "content": [{"text": f"Generate {num_variations} related search phrases for: '{query}'"}],
                }
            ]
            model = AIModel.objects.get(name="nova micro")
            model_id = model.model_id
            # First call to get tool use
            response = client.converse(modelId=model_id, messages=messages, toolConfig=tool_config, system=system)

            output_message = response["output"]["message"]
            stop_reason = response["stopReason"]
            if stop_reason == "tool_use":
                # Extract tool use from response
                tool_use = None
                for content in output_message["content"]:
                    if "toolUse" in content:
                        tool_use = content["toolUse"]
                        break

                if tool_use and tool_use["name"] == "expand_search_query":
                    variations = tool_use["input"].get("variations", [])

                    if isinstance(variations, list) and len(variations) > 0:
                        print(f"  Generated variations: {variations}")
                        return [query] + variations[:num_variations]

            # If tool wasn't used or failed, return original query
            print(f"  Query expansion didn't use tool (stop_reason: {stop_reason}), using original query only")
            return [query]

        except Exception as e:
            print(f"  Query expansion failed: {e}")
            import traceback

            traceback.print_exc()
            return [query]  # Fallback to original only

    def lookup_codes_hierarchical(
        self,
        query: str,
        code_type: Literal["naics", "psc", "cfda", "tas"],
        top_k: int = 20,
    ) -> dict[str, Any]:
        """Search with hierarchy awareness"""

        # First, do normal search
        results = self.lookup_codes(query, code_type, top_k=top_k * 2)

        # Extract parent codes from results
        parent_codes = set()
        for result in results["results"]:
            code = result["identifier"]
            if len(code) >= 4:
                parent_codes.add(code[:4])

        # Fetch parent codes from database
        config = CODE_TYPE_CONFIGS[code_type]
        Model = config.model_class

        parents = Model.objects.filter(**{f"{config.code_field}__in": list(parent_codes)})

        # Build a map of existing results by code for easy lookup
        existing_results = {r["identifier"]: r for r in results["results"]}

        for parent in parents:
            parent_code = getattr(parent, config.code_field)
            parent_description = getattr(parent, config.description_field, None)

            # Calculate score based on children
            child_scores = [
                r["score"]
                for r in results["results"]
                if r["identifier"].startswith(parent_code) and r["identifier"] != parent_code
            ]

            if child_scores:
                # Boost parent above best child
                boosted_score = max(child_scores) * 1.1

                if parent_code in existing_results:
                    # Parent already exists - boost its score if children scored higher
                    existing_score = existing_results[parent_code].get("score", 0)
                    existing_results[parent_code]["score"] = max(existing_score, boosted_score)
                    existing_results[parent_code]["is_parent"] = True
                    existing_results[parent_code]["child_count"] = len(child_scores)
                    print(
                        f"  Boosted existing parent {parent_code}: {existing_score:.3f} → {existing_results[parent_code]['score']:.3f}"
                    )
                else:
                    # Parent not in results - add it
                    new_parent_result = self._transform_to_selected_code(
                        parent_code, parent_description, boosted_score, config
                    )
                    new_parent_result["score"] = boosted_score
                    new_parent_result["is_parent"] = True
                    new_parent_result["child_count"] = len(child_scores)
                    results["results"].append(new_parent_result)
                    print(f"  Added parent {parent_code}: {boosted_score:.3f} ({len(child_scores)} children)")

        # Re-sort and limit
        results["results"] = sorted(results["results"], key=lambda x: x.get("score", 0), reverse=True)[:top_k]

        return results


lookup_codes_tool = AITool(
    function=CodeLookupTool().lookup_codes_hierarchical,
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
