import pytest
from django.db import IntegrityError
from django.utils import timezone

from usaspending_api.llm.models.db_models import AIModel, Message, Prompts, Session, ToolUse


@pytest.mark.django_db
class TestAIModel:
    def test_create_ai_model(self):
        """Test creating an AIModel instance"""
        ai_model = AIModel.objects.create(
            name="claude 3.5",
            model_id="anthropic.claude-3-5-sonnet",
            provider="anthropic"
        )

        assert ai_model.id is not None
        assert ai_model.name == "claude 3.5"
        assert ai_model.model_id == "anthropic.claude-3-5-sonnet"
        assert ai_model.provider == "anthropic"

    def test_ai_model_str_representation(self):
        """Test __str__ method of AIModel"""
        ai_model = AIModel.objects.create(
            name="claude 3.5",
            model_id="anthropic.claude-3-5-sonnet",
            provider="anthropic"
        )

        assert str(ai_model) == "claude 3.5 - anthropic.claude-3-5-sonnet (anthropic)"

    def test_ai_model_ordering(self):
        """Test AIModel ordering by -id"""
        model1 = AIModel.objects.create(name="Model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="Model 2", model_id="model-2", provider="provider2")
        model3 = AIModel.objects.create(name="Model 3", model_id="model-3", provider="provider3")

        models = list(AIModel.objects.all())
        assert models[0] == model3
        assert models[1] == model2
        assert models[2] == model1


@pytest.mark.django_db
class TestPrompts:
    def test_create_prompt(self):
        """Test creating a Prompts instance"""
        prompt = Prompts.objects.create(
            name="test_prompt",
            description="Test prompt",
            text="You are a helpful assistant"
        )

        assert prompt.id is not None
        assert prompt.name == "test_prompt"
        assert prompt.description == "Test prompt"
        assert prompt.text == "You are a helpful assistant"
        assert prompt.created_at is not None

    def test_prompt_str_representation(self):
        """Test __str__ method of Prompts"""
        prompt = Prompts.objects.create(
            name="test_prompt",
            description="Test prompt",
            text="You are a helpful assistant"
        )

        assert str(prompt) == "test_prompt"

    def test_prompt_created_at_auto_set(self):
        """Test that created_at is automatically set"""
        before = timezone.now()
        prompt = Prompts.objects.create(
            name="test_prompt",
            description="Test prompt",
            text="You are a helpful assistant"
        )
        after = timezone.now()

        assert before <= prompt.created_at <= after

    def test_prompt_unique_name(self):
        """Test that prompt names must be unique"""
        Prompts.objects.create(
            name="unique_prompt",
            description="First prompt",
            text="Text 1"
        )

        with pytest.raises(IntegrityError):
            Prompts.objects.create(
                name="unique_prompt",
                description="Second prompt",
                text="Text 2"
            )


@pytest.mark.django_db
class TestSession:
    def test_create_session(self):
        """Test creating a Session instance"""
        ai_model = AIModel.objects.create(
            name="claude 3.5",
            model_id="anthropic.claude-3-5-sonnet",
            provider="anthropic"
        )
        prompt = Prompts.objects.create(
            name="system_prompt",
            description="System prompt",
            text="You are a search assistant"
        )

        session = Session.objects.create(
            ai_model=ai_model,
            system_prompt=prompt,
            tools=["tool1", "tool2"]
        )

        assert session.id is not None
        assert session.ai_model == ai_model
        assert session.system_prompt == prompt
        assert session.tools == ["tool1", "tool2"]
        assert session.started_at is not None
        assert session.ended_at is None
        assert session.feedback is None

    def test_session_without_ai_model(self):
        """Test creating a session without an AI model"""
        session = Session.objects.create(tools=[])
        assert session.ai_model is None

    def test_session_tools_default_value(self):
        """Test that tools defaults to empty list"""
        session = Session.objects.create()
        assert session.tools == []

    def test_session_feedback_states(self):
        """Test session feedback boolean states"""
        # Positive feedback
        session_positive = Session.objects.create(feedback=True)
        assert session_positive.feedback is True

        # Negative feedback
        session_negative = Session.objects.create(feedback=False)
        assert session_negative.feedback is False

        # No feedback
        session_none = Session.objects.create(feedback=None)
        assert session_none.feedback is None

    def test_session_end_tracking(self):
        """Test session start and end time tracking"""
        session = Session.objects.create()
        assert session.started_at is not None
        assert session.ended_at is None

        # End the session
        session.ended_at = timezone.now()
        session.save()
        assert session.ended_at is not None

    def test_session_str_representation(self):
        """Test __str__ method of Session"""
        session = Session.objects.create()
        expected_start = session.started_at.strftime('%Y-%m-%d %H:%M')
        expected = f"Session {session.id} - No Model ({expected_start})"
        assert str(session) == expected

    def test_session_ordering(self):
        """Test Session ordering by -started_at"""
        session1 = Session.objects.create()
        session2 = Session.objects.create()
        session3 = Session.objects.create()

        sessions = list(Session.objects.all())
        assert sessions[0] == session3
        assert sessions[1] == session2
        assert sessions[2] == session1

    def test_session_cascade_on_ai_model_delete(self):
        """Test that session.ai_model is set to NULL when AIModel is deleted"""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test-provider"
        )
        session = Session.objects.create(ai_model=ai_model)

        ai_model.delete()
        session.refresh_from_db()
        assert session.ai_model is None

    def test_session_cascade_on_prompt_delete(self):
        """Test that session.system_prompt is set to NULL when Prompts is deleted"""
        prompt = Prompts.objects.create(
            name="test_prompt",
            description="Test",
            text="Test prompt"
        )
        session = Session.objects.create(system_prompt=prompt)

        prompt.delete()
        session.refresh_from_db()
        assert session.system_prompt is None


@pytest.mark.django_db
class TestMessage:
    def test_create_message(self):
        """Test creating a Message instance"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="user",
            message="Hello, assistant!",
            order=0
        )

        assert message.id is not None
        assert message.session == session
        assert message.role == "user"
        assert message.message == "Hello, assistant!"
        assert message.order == 0
        assert message.created_at is not None
        assert message.input_tokens == 0
        assert message.output_tokens == 0
        assert message.latency == 0

    def test_message_with_token_counts(self):
        """Test creating a message with token counts"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="assistant",
            message="Response text",
            order=1,
            input_tokens=100,
            output_tokens=50,
            latency=250
        )

        assert message.input_tokens == 100
        assert message.output_tokens == 50
        assert message.latency == 250

    def test_message_str_representation(self):
        """Test __str__ method of Message"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="user",
            message="Test message",
            order=0
        )

        assert str(message) == "user (Order 0): Test message"

    def test_message_ordering(self):
        """Test Message ordering by session and order"""
        session = Session.objects.create()
        msg3 = Message.objects.create(session=session, role="user", message="Third", order=2)
        msg1 = Message.objects.create(session=session, role="user", message="First", order=0)
        msg2 = Message.objects.create(session=session, role="assistant", message="Second", order=1)

        messages = list(Message.objects.filter(session=session))
        assert messages[0] == msg1
        assert messages[1] == msg2
        assert messages[2] == msg3

    def test_message_unique_session_order(self):
        """Test that session and order combination is unique"""
        session = Session.objects.create()
        Message.objects.create(session=session, role="user", message="First", order=0)

        with pytest.raises(IntegrityError):
            Message.objects.create(session=session, role="assistant", message="Duplicate", order=0)

    def test_message_cascade_delete_with_session(self):
        """Test that messages are deleted when session is deleted"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="user",
            message="Test",
            order=0
        )

        session.delete()
        assert not Message.objects.filter(id=message.id).exists()


@pytest.mark.django_db
class TestToolUse:
    def test_create_tool_use(self):
        """Test creating a ToolUse instance"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="assistant",
            message="Using tool",
            order=0
        )
        tool_use = ToolUse.objects.create(
            message=message,
            name="lookup_location",
            tool_input={"query": "Texas"},
            result={"identifier": "USA_TX"}
        )

        assert tool_use.id is not None
        assert tool_use.message == message
        assert tool_use.name == "lookup_location"
        assert tool_use.tool_input == {"query": "Texas"}
        assert tool_use.result == {"identifier": "USA_TX"}
        assert tool_use.created_at is not None

    def test_tool_use_str_representation(self):
        """Test __str__ method of ToolUse"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="assistant",
            message="Using tool",
            order=0
        )
        tool_use = ToolUse.objects.create(
            message=message,
            name="lookup_location",
            tool_input={"query": "Texas"},
            result={"identifier": "USA_TX"}
        )

        expected_time = tool_use.created_at.strftime('%Y-%m-%d %H:%M:%S')
        assert str(tool_use) == f"lookup_location - {expected_time}"

    def test_tool_use_cascade_delete_with_message(self):
        """Test that tool uses are deleted when message is deleted"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="assistant",
            message="Using tool",
            order=0
        )
        tool_use = ToolUse.objects.create(
            message=message,
            name="test_tool",
            tool_input={},
            result={}
        )

        message.delete()
        assert not ToolUse.objects.filter(id=tool_use.id).exists()

    def test_multiple_tool_uses_per_message(self):
        """Test that a message can have multiple tool uses"""
        session = Session.objects.create()
        message = Message.objects.create(
            session=session,
            role="assistant",
            message="Using multiple tools",
            order=0
        )

        tool1 = ToolUse.objects.create(
            message=message,
            name="tool1",
            tool_input={"param": "value1"},
            result={"result": "output1"}
        )
        tool2 = ToolUse.objects.create(
            message=message,
            name="tool2",
            tool_input={"param": "value2"},
            result={"result": "output2"}
        )

        assert message.tool_uses.count() == 2
        assert tool1 in message.tool_uses.all()
        assert tool2 in message.tool_uses.all()


@pytest.mark.django_db
class TestModelRelationships:
    def test_session_relationships(self):
        """Test Session reverse relationships"""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test-provider"
        )
        prompt = Prompts.objects.create(
            name="test_prompt",
            description="Test prompt",
            text="Test"
        )

        Session.objects.create(ai_model=ai_model, system_prompt=prompt)
        Session.objects.create(ai_model=ai_model, system_prompt=prompt)

        assert ai_model.sessions.count() == 2
        assert prompt.sessions.count() == 2

    def test_message_relationships(self):
        """Test Message reverse relationships"""
        session = Session.objects.create()

        msg1 = Message.objects.create(session=session, role="user", message="Hi", order=0)
        msg2 = Message.objects.create(session=session, role="assistant", message="Hello", order=1)

        assert session.messages.count() == 2
        assert list(session.messages.all()) == [msg1, msg2]

    def test_full_conversation_flow(self):
        """Test a complete conversation flow with all models"""
        # Setup
        ai_model = AIModel.objects.create(
            name="claude",
            model_id="claude-3",
            provider="anthropic"
        )
        prompt = Prompts.objects.create(
            name="search_assistant",
            description="Search assistant",
            text="You are a helpful search assistant"
        )

        # Create session
        session = Session.objects.create(
            ai_model=ai_model,
            system_prompt=prompt,
            tools=["lookup_location", "advanced_search"]
        )

        # Create user message
        Message.objects.create(
            session=session,
            role="user",
            message="Find contracts in Texas",
            order=0
        )

        # Create assistant message with tool use
        assistant_msg = Message.objects.create(
            session=session,
            role="assistant",
            message="I'll help you search",
            order=1,
            input_tokens=50,
            output_tokens=30,
            latency=200
        )

        ToolUse.objects.create(
            message=assistant_msg,
            name="lookup_location",
            tool_input={"query": "Texas"},
            result={"identifier": "USA_TX"}
        )
