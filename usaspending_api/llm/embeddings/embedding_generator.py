import json
import logging
import boto3
from botocore.config import Config
from usaspending_api.llm.models.db_models import AIModel

logger = logging.getLogger(__name__)


class EmbeddingGenerator:

    def __init__(
        self,
        model: AIModel | None = None,
        dimensions: int = 256,
        normalize: bool = True,
    ):

        self.model = model if model else AIModel.objects.get(name="titan")
        self.dimensions = dimensions
        self.normalize = normalize

        # Configure boto3 with retries
        config = Config(retries={"max_attempts": 3, "mode": "adaptive"})
        self.client = boto3.client(service_name="bedrock-runtime", config=config)

    def generate_embedding(self, text: str) -> list[float] | None:
        """Generate embedding for a single text"""
        if not text or not text.strip():
            return None

        max_input = 8192  # Titan v2 max input: 8192 tokens
        try:
            request_body = {
                "inputText": text[:max_input],
                "dimensions": self.dimensions,
                "normalize": self.normalize,
            }

            response = self.client.invoke_model(
                modelId=self.model.model_id,
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json",
            )

            response_body = json.loads(response["body"].read())
            return response_body.get("embedding")

        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            return None

    def generate_embeddings_batch(self, texts: list[str], batch_size: int = 10) -> list[list[float] | None]:
        """
        Generate embeddings for multiple texts with batching
        Note: Titan doesn't have native batch API, so we batch requests
        """
        embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batch_embeddings = [self.generate_embedding(text) for text in batch]
            embeddings.extend(batch_embeddings)

        return embeddings
