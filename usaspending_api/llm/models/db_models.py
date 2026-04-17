from django.db import models


class AIModel(models.Model):
    name = models.CharField(max_length=100)
    model_id = models.CharField(max_length=100)
    provider = models.CharField(max_length=100)


class Prompts(models.Model):
    description = models.TextField()
    text = models.TextField()


class Session(models.Model):
    ai_model = models.ForeignKey(AIModel, on_delete=models.SET_NULL, null=True)
    tools = models.JSONField(default=dict)
    system_prompt = models.ForeignKey(Prompts, on_delete=models.SET_NULL, null=True)
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    feedback = models.BooleanField(default=None, null=True, blank=True, help_text="positive=True, negative=False")


class Message(models.Model):
    session = models.ForeignKey(Session, on_delete=models.CASCADE)
    role = models.CharField(max_length=100)
    message = models.TextField()
    order = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    input_tokens = models.IntegerField(default=0)
    output_tokens = models.IntegerField(default=0)
    total_tokens = models.IntegerField(default=0)
    latency = models.IntegerField(default=0, help_text="latency in milliseconds")


class ToolUse(models.Model):
    message = models.ForeignKey(Message, on_delete=models.CASCADE)
    name = models.CharField(max_length=200)
    input = models.JSONField()
    result = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)


class LLMSearchQuery(models.Model):
    user_query = models.TextField()
    session = models.ForeignKey(Session, on_delete=models.CASCADE)
