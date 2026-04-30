from django.db import models


class AIModel(models.Model):
    name = models.CharField(max_length=100)
    model_id = models.CharField(max_length=100)
    provider = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.name} ({self.provider})"

    class Meta:
        db_table = "ai_model"
        ordering = ["-id"]


class Prompts(models.Model):
    description = models.TextField()
    text = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.description[:100]

    class Meta:
        db_table = "prompts"


class Session(models.Model):
    ai_model = models.ForeignKey(AIModel, on_delete=models.SET_NULL, null=True, related_name="sessions")
    tools = models.JSONField(default=list)
    system_prompt = models.ForeignKey(Prompts, on_delete=models.SET_NULL, null=True, related_name="sessions")
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    feedback = models.BooleanField(default=None, null=True, blank=True, help_text="positive=True, negative=False")

    def __str__(self):
        model_name = self.ai_model.name if self.ai_model else 'No Model'
        timestamp = self.started_at.strftime('%Y-%m-%d %H:%M')
        return f"Session {self.id} - {model_name} ({timestamp})"

    class Meta:
        db_table = "session"
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["-started_at"]),
            models.Index(fields=["feedback"]),
        ]


class Message(models.Model):
    session = models.ForeignKey(Session, on_delete=models.CASCADE, related_name="messages")
    role = models.CharField(max_length=100)
    message = models.TextField()
    order = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    input_tokens = models.IntegerField(default=0)
    output_tokens = models.IntegerField(default=0)
    total_tokens = models.IntegerField(default=0)
    latency = models.IntegerField(default=0, help_text="latency in milliseconds")

    def __str__(self):
        preview = self.message[:50] + "..." if len(self.message) > 50 else self.message
        return f"{self.role} (Order {self.order}): {preview}"

    class Meta:
        db_table = "message"
        ordering = ["session", "order"]
        indexes = [
            models.Index(fields=["session", "order"]),
            models.Index(fields=["created_at"]),
        ]
        unique_together = [["session", "order"]]


class ToolUse(models.Model):
    message = models.ForeignKey(Message, on_delete=models.CASCADE, related_name="tool_uses")
    name = models.CharField(max_length=200)
    tool_input = models.JSONField()
    result = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} - {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}"

    class Meta:
        db_table = "tool_use"
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["created_at"]),
        ]


class LLMSearchQuery(models.Model):
    user_query = models.TextField()
    session = models.ForeignKey(Session, on_delete=models.CASCADE, related_name="search_queries")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        preview = self.user_query[:75] + "..." if len(self.user_query) > 75 else self.user_query
        return f"Query {self.id}: {preview}"

    class Meta:
        db_table = "llm_search_query"
        indexes = [
            models.Index(fields=["-created_at"]),
        ]
