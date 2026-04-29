import json
import logging
from datetime import datetime
from typing import Generator

from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework import status

from usaspending_api.llm.models.db_models import Session, Message, LLMSearchQuery, AIModel, Prompts
from usaspending_api.llm.assistants.search_assistant import SearchAssistant
from usaspending_api.llm.tools.advanced_search_filter import create_advanced_search_filter
from usaspending_api.llm.tools.lookup_location import lookup_location_tool


logger = logging.getLogger(__name__)


class StreamingSearchView(APIView):

    def post(self, request: Request):

        query_text = request.data.get("query")
        if not query_text:
            return Response({"error": "Query text is required"}, status=status.HTTP_400_BAD_REQUEST)

        ai_model = AIModel.objects.get(name="nova micro")
        system_prompt = Prompts.objects.get(id=1)
        current_date_prompt = f"\n The current date is {datetime.today().strftime('%Y-%m-%d')}"
        system_prompt.description += current_date_prompt
        session = Session.objects.create(
            ai_model=ai_model,
            system_prompt=system_prompt,
            tools=["create_advanced_search_filter", "search_for_location"],
        )

        search_query = LLMSearchQuery.objects.create(user_query=query_text, session=session)

        # Create streaming response
        response = StreamingHttpResponse(
            self.stream_search(session, search_query, query_text, ai_model), content_type="text/event-stream"
        )
        response["Cache-Control"] = "no-cache"
        response["X-Accel-Buffering"] = "no"
        response["Access-Control-Allow-Origin"] = "*"

        return response

    def stream_search(
        self, session: Session, search_query: LLMSearchQuery, query_text: str, ai_model: AIModel
    ) -> Generator[str, None, None]:

        try:
            # Send initial event
            yield self.format_sse(
                {
                    "type": "start",
                    "session_id": session.id,
                    "query_id": search_query.id,
                    "query_text": query_text,
                }
            )

            # Create streaming assistant
            assistant = SearchAssistant(
                model=ai_model,
                tools=[create_advanced_search_filter, lookup_location_tool],
                session=session,
                system_message=session.system_prompt.text if session.system_prompt else None,
            )

            # Execute search with streaming
            for event in assistant.search(query_text):
                # TODO add the token counts and latency from the response to each event that is emitted
                if event["type"] == "tool":
                    yield self.format_sse({"type": "tool", "tool_logging": event["message"]})
                elif event["type"] == "hash":
                    yield self.format_sse({"type": "result", "hash": event["result"]})
            session.ended_at = timezone.now()
            session.save()

        except Exception as e:
            yield self.format_sse({"type": "error", "error": str(e), "session_id": session.id})
            session.ended_at = timezone.now()
            session.save()

    def format_sse(self, data: dict) -> str:
        """Format data as Server-Sent Event"""
        return f"{json.dumps(data)}\n"
