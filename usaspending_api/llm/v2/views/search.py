import json
import logging
import time
from typing import Generator

from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework import status

from usaspending_api.llm.models.db_models import Session, Message, ToolUse, LLMSearchQuery, AIModel, Prompts
from usaspending_api.llm.assistants.search_assistant import SearchAssistant
from usaspending_api.llm.tools.advanced_search_filter import create_advanced_search_filter
from usaspending_api.llm.tools.location_filter import search_for_location


logger = logging.getLogger(__name__)


class StreamingSearchView(APIView):

    def post(self, request: Request):

        query_text = request.data.get("query")
        if not query_text:
            return Response({"error": "Query text is required"}, status=status.HTTP_400_BAD_REQUEST)

        ai_model = AIModel.objects.get(name="nova micro")
        system_prompt = Prompts.objects.get(id=1)
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

        message_order = 0
        current_message = None

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

            # Create user message
            user_message = Message.objects.create(session=session, role="user", message=query_text, order=message_order)
            message_order += 1

            # Create streaming assistant
            assistant = SearchAssistant(
                model=ai_model,
                tools=[create_advanced_search_filter, search_for_location],
                system_message=session.system_prompt.text if session.system_prompt else None,
            )

            # Execute search with streaming
            for event in assistant.search(query_text):
                if event["type"] == "tool":
                    yield event["message"]
                elif event["type"] == "hash":
                    yield event["result"]

                # elif event["type"] == "content_start":
                #     # Create assistant message
                #     current_message = Message.objects.create(
                #         session=session, role="assistant", message="", order=message_order
                #     )
                #     message_order += 1
                #
                # elif event["type"] == "content":
                #     # Stream content chunks
                #     if current_message:
                #         current_message.message += event["content"]
                #         current_message.save()
                #
                #     yield self.format_sse({"type": "content", "content": event["content"]})
                #
                #

            # # Update final message with token counts
            # if current_message:
            # current_message.input_tokens = total_input_tokens
            # current_message.output_tokens = total_output_tokens
            # current_message.total_tokens = total_input_tokens + total_output_tokens
            # current_message.latency = latency_ms
            # current_message.save()

            # Update session end time
            session.ended_at = timezone.now()
            session.save()
            #
            # # Send completion event
            # yield self.format_sse(
            #     {
            #         "type": "complete",
            #         "session_id": session.id,
            #         "query_id": search_query.id,
            #         "latency_ms": latency_ms,
            #         "input_tokens": total_input_tokens,
            #         "output_tokens": total_output_tokens,
            #         "total_tokens": total_input_tokens + total_output_tokens,
            #     }
            # )

        except Exception as e:
            # Send error event
            yield self.format_sse({"type": "error", "error": str(e), "session_id": session.id})

            # Update session
            session.ended_at = timezone.now()
            session.save()

    def format_sse(self, data: dict) -> str:
        """Format data as Server-Sent Event"""
        return f"data: {json.dumps(data)}\n\n"
