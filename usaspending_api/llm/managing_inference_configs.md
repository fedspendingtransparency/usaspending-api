# Managing AI Inference Configurations

This guide explains how to configure and manage inference parameters for AI models in the LLM application.

## Overview

Inference configurations (explained below) are managed on a per Request basis. Updating these configurations
mid-conversation will not result in any changes. You must create a new Request context to see the changes.

* **NOTE**: Changing configs on an AI Model will update that model for **ALL USERS** (i.e., it's a global change).
* **NOTE**: Management commands update inference configs on the AI Models in the database, they do not choose which 
model is used by the AI Assistants. Make sure if you are testing config updates, you target the model actually in use.

Inference configurations control how the LLM generates responses:
- **temperature**: Controls randomness (0.0 = deterministic, 1.0 = creative).
- **topP**: Token sampling threshold (0.0 = threshold reached with 1 token, 1.0 = all tokens considered).
- **maxTokens**: Maximum tokens to generate in a response.
- **stopSequences**: List of strings that halt generation if/when encountered.

* **NOTE**: Temperature and Top-P control probability evaluations across the tokens the model will consider when
generating a response. See https://tomarcher.io/posts/temperature-top-p-creativity-knobs/ for more details. If a
fully deterministic model is desired, it is recommended we set temperature to 0.0 - 0.3 and topP to 1.0 (or omit it) 
to allow the model to consider all tokens when generating a response (topP = 1.0) and always choose the highest 
probability token from the full vocabulary list (temperature = 0.0).

## Default Values

If no inference config is specified, these defaults are used:
```json
{
  "temperature": 0.0,   # Deterministic.
  "topP": 1.0,          # All tokens considered (allows the 0.0 temperature to work with the full vocabulary).
  "maxTokens": 2048,    # A small/controlled output size for smaller/simpler models.
  "stopSequences": []
}
```

These defaults are optimized for deterministic, consistent and focused responses.

## Configuration Methods

### 1. Via Fixtures (Recommended for Initial Setup)

Edit `usaspending_api/llm/fixtures/ai_models.yaml`:

```yaml
- model: llm.aimodel
  pk: 1
  fields:
    name: claude 4.5
    model_id: anthropic.claude-sonnet-4-5-20250929-v1:0
    provider: anthropic
    inference_config:
      temperature: 0.0
      topP: 1.0
      maxTokens: 2048
      stopSequences: []
```

Then load fixtures:
```bash
python manage.py load_llm_fixtures
```

### 2. Via Management Command (Recommended for Updates)

#### List all models and their configs:
```bash
python manage.py update_inference_config --list
```

#### Update individual parameters:
```bash
# Update temperature:
python manage.py update_inference_config --model-name "claude 4.5" --temperature 0.5

# Update multiple parameters:
python manage.py update_inference_config \
  --model-id "anthropic.claude-3-5-sonnet" \
  --temperature 0.7 \
  --top-p 0.9 \
  --max-tokens 4096

# Update stop sequences:
python manage.py update_inference_config \
  --model-name "claude 4.5" \
  --stop-sequences "Human:,User:,\\n\\n"
```

#### Update with full JSON config:
```bash
python manage.py update_inference_config \
  --model-name "claude 4.5" \
  --config-json '{"temperature": 0.8, "topP": 0.95, "maxTokens": 8192, "stopSequences": ["Human:", "User:"]}'
```

#### Clear config (revert to defaults):
```bash
python manage.py update_inference_config --model-name "claude 4.5" --clear
```

### 3. Via Django Shell

```python
from usaspending_api.llm.models.db_models import AIModel

# Get model:
model = AIModel.objects.get(name="claude 4.5")

# Update config (example):
model.inference_config = {
    "temperature": 0.5,
    "topP": 0.8,
    "maxTokens": 4096,
    "stopSequences": ["Human:", "\n\nUser:"],
}
model.save()

# View current config:
print(model.inference_config)
```

### 4. In Tests

#### Integration tests (with database):
```python
@pytest.mark.django_db
def test_custom_inference():
    ai_model = AIModel.objects.create(
        name="test model",
        model_id="test-id",
        provider="test",
        inference_config={
            "temperature": 0.8,
            "topP": 0.9,
            "maxTokens": 512,
            "stopSequences": [],
        }
    )
    
    session = Session.objects.create(ai_model=ai_model)
    assistant = FilterSearchAssistant(
        model=ai_model,
        tools=[],
        session=session
    )
    
    assert assistant.inference_config["temperature"] == 0.8
```

#### Unit tests (with mocks):
```python
def test_with_mock_config():
    mock_model = Mock(spec=AIModel)
    mock_model.model_id = "test-id"
    mock_model.inference_config = {
        "temperature": 0.3,
        "topP": 0.5,
        "maxTokens": 1024,
        "stopSequences": ["Human:"],
    }
    
    assistant = FilterSearchAssistant(
        model=mock_model,
        tools=[],
        session=mock_session
    )
    
    assert assistant.inference_config["temperature"] == 0.3
```

## Validations

The management command automatically validates all inference parameters:

### Temperature
- **Range**: 0.0 to 1.0 (inclusive)
- **Type**: Float or integer
- **Error example**: `Invalid temperature: 1.5. Must be between 0.0 and 1.0`

### Top P
- **Range**: 0.0 to 1.0 (inclusive)
- **Type**: Float or integer
- **Error example**: `Invalid top-p: 2.0. Must be between 0.0 and 1.0`

### Max Tokens
- **Range**: Must be positive (> 0)
- **Type**: Integer only
- **Error example**: `Invalid max-tokens: -100. Must be a positive integer`

### Stop Sequences
- **Type**: List of strings
- **Format**: Comma-separated when using CLI (e.g., `"Human:,User:,\\n\\n"`)
- **Special characters**: Use `\\n` for newlines in CLI
- **Error example**: `Invalid stop-sequences type: str. Must be a list`

### JSON Config
When using `--config-json`, the command validates:
- All parameter types are correct
- All parameter values are within valid ranges
- JSON is properly formatted

## Troubleshooting

### Config not being used
1. Check that the model has config: `python manage.py update_inference_config --list`.
2. Verify assistant is using model's config (check tests).
3. Ensure migration has been applied: `python manage.py migrate llm`.

### Validation errors
The command will reject invalid values before saving to the database:
- temperature and topP must be 0.0 - 1.0.
- maxTokens must be a positive integer.
- All types must match expected types (no strings for numeric values).

### Empty config
If `inference_config` is empty `{}`, the assistant will use default values. This is for backward compatibility.

## See Also

- `usaspending_api/llm/assistants/filter_search.py` - Implementation
- `usaspending_api/llm/tests/integration/test_inference_config.py` - Test examples
- `usaspending_api/llm/models/db_models.py` - Model definition
