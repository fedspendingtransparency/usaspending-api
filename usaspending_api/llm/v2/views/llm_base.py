import json
import logging
import os
import uuid

from django.http import StreamingHttpResponse

from rest_framework.views import APIView

from usaspending_api.llm.models.db_models import AIModel

logger = logging.getLogger(__name__)


class LLMBase(APIView):
    """
    Base class for LLM-powered endpoints.
    
    Provides shared functionality for streaming responses, error handling,
    and AI model management following the pattern set in agency_base.py.
    """
    
    # Default AI model name (can be overridden by LLM_DEFAULT_MODEL env variable).
    DEFAULT_MODEL_NAME = "nova micro"
    
    def _get_ai_model(self, model_name: str = None) -> AIModel:
        """
        Get an AI model instance from the database.
        
        Args:
            model_name: Optional model name. If not provided, uses environment variable
                       LLM_DEFAULT_MODEL or falls back to DEFAULT_MODEL_NAME.
        
        Returns:
            AIModel instance.
            
        Raises:
            ValueError: If the specified model is not found.
        """
        if not model_name:
            model_name = os.environ.get("LLM_DEFAULT_MODEL", self.DEFAULT_MODEL_NAME)
        
        ai_model = AIModel.objects.filter(name=model_name).first()
        if not ai_model:
            raise ValueError(f"AI model '{model_name}' not found in database")
        
        return ai_model
    

    def _ndjson_format(self, event: dict) -> str:
        """
        Format an event dictionary as a newline-delimited JSON (NDJSON) string.
        
        Args:
            event: Dictionary containing event data.
            
        Returns:
            JSON string with newline terminator for NDJSON streaming (application/x-ndjson).
        """
        return json.dumps(event) + "\n"
    

    def _error_response(self, message: str, search_id: str = None) -> StreamingHttpResponse:
        """
        Generate a streaming error response in newline-delimited JSON (NDJSON) format.
        
        Args:
            message: Error message to return to the client.
            search_id: Optional session/search ID. If None, generates a temporary UUID.
            
        Returns:
            StreamingHttpResponse with error event.
        """
        error_event = {
            "search_id": search_id if search_id else str(uuid.uuid4()),
            "type": "search_error",
            "message": message
        }
        
        def error_stream():
            yield self._ndjson_format(error_event)
        
        response = StreamingHttpResponse(
            error_stream(),
            content_type="application/x-ndjson"
        )
        # Disable webserver caching/buffering to enable pass-through behavior of chunks.
        response["Cache-Control"] = "no-cache"
        response["X-Accel-Buffering"] = "no"
        
        return response
