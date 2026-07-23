import logging
from typing import Generator

from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.request import Request

from usaspending_api.common.api_request_utils import LLMAPIKeyHandler
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import Prompts, Session
from usaspending_api.llm.tools.lookup_location import lookup_location_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool
from usaspending_api.llm.v2.views.llm_base import LLMBase

logger = logging.getLogger(__name__)


class FilterSearchViewSet(LLMBase):
    """
    This endpoint provides a streaming response for LLM-powered search operations with advanced filtering capabilities.
    The response is delivered as a series of JSON chunks, allowing real-time updates on search progress,
    tool execution, and results.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/llm/filter_search.md"

    # Define a list of allowed AI tools to pass to the assistant.
    tools = [
        lookup_location_tool,
        lookup_recipient_tool,
    ]

    @LLMAPIKeyHandler.require_api_key
    def post(self, request: Request) -> StreamingHttpResponse:

        # Accept a string sanitized as search input.
        models = [
            {"name": "filter_search", "key": "query", "type": "text", "text_type": "search", "min": 1, "max": 1000}
        ]

        try:
            # Validate request and retrieve AI model.
            validated_request_data = TinyShield(models).block(request.data)
            query = validated_request_data["query"]
            ai_model = self._get_ai_model()

            # Get available tools.
            tools = self.tools

            # Retrieve system prompt from database (fall back to Assistant's default if not found).
            system_prompt = None
            try:
                system_prompt = Prompts.objects.get(name="initial")
            except Prompts.DoesNotExist:
                logger.warning("System prompt not found in database, using Assistant's default.")

            # Instantiate session.
            session = Session.objects.create(
                ai_model=ai_model,
                tools=[tool.description.name for tool in tools],
                system_prompt=system_prompt,
            )

            # Create assistant with appropriate arguments.
            assistant_kwargs = {
                "model": ai_model,
                "tools": tools,
                "session": session,
            }
            # If system_prompt is set, override the Assistant's default prompt.
            if system_prompt:
                assistant_kwargs["system_message"] = system_prompt.text

            assistant = FilterSearchAssistant(**assistant_kwargs)

            def event_stream() -> Generator[str, None, None]:
                try:
                    for event in assistant.search(query):
                        yield self._ndjson_format(event)
                except Exception as e:
                    logger.error(f"Error during filter search: {str(e)}", exc_info=True)
                    error_event = {
                        "search_id": str(session.id),
                        "type": "search_error",
                        "message": f"An error occurred: {str(e)}"
                    }
                    yield self._ndjson_format(error_event)
                finally:
                    # Update session end time when stream completes (success or error).
                    session.ended_at = timezone.now()
                    session.save(update_fields=["ended_at"])

            # Craft Response stream.
            response = StreamingHttpResponse(
                event_stream(),
                content_type="application/x-ndjson"
            )
            # Disable webserver caching/buffering to enable pass-through behavior of chunks in the stream.
            response["Cache-Control"] = "no-cache"
            response["X-Accel-Buffering"] = "no"

            return response

        except Exception as e:
            logger.error(f"Error initializing filter search: {str(e)}", exc_info=True)
            return self._error_response(f"Failed to initialize search: {str(e)}", search_id=None)
