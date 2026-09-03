# Managing AI Assistant Configurations

This guide explains how to configure and manage AI Assistants, including their inference parameters, system prompts, and AI models.

## Overview

AI Assistants are configured entities that combine an AI model, system prompt, and inference configurations. Each Assistant can be independently configured and updated using Django management commands.

**Important Notes:**
- Inference configurations are managed on a per-Assistant basis
- Updating configurations mid-conversation will not result in changes; you must create a new Request context
- Changes to an Assistant affect all users of that Assistant (global changes)

## Inference Configuration Parameters

Inference configurations control how the LLM generates responses:
- **temperature**: Controls randomness (0.0 = deterministic, 1.0 = creative)
- **topP**: Token sampling threshold (0.0 = threshold reached with 1 token, 1.0 = all tokens considered)
- **maxTokens**: Maximum tokens to generate in a response
- **stopSequences**: List of strings that halt generation if/when encountered

**Note on Temperature and Top-P**: These control probability evaluations across the tokens the model will consider when generating a response. See [this article](https://tomarcher.io/posts/temperature-top-p-creativity-knobs/) for more details. For a fully deterministic model, set temperature to 0.0-0.3 and topP to 1.0 (or omit it) to allow the model to consider all tokens when generating a response and always choose the highest probability token.

## Default Values

If no inference config is specified, these defaults are used:
```json
{
  "temperature": 0.0,   // Deterministic
  "topP": 1.0,          // All tokens considered
  "maxTokens": 5000,    // Controlled output size
  "stopSequences": []
}
```

These defaults are optimized for deterministic, consistent, and focused responses.

**Using Model Defaults**: Each inference parameter can be set to `null` to allow the AI model to use its own default value. When `null`, that parameter will be omitted from the inference request entirely, allowing the model's native defaults to take effect. This is useful when you want to leverage model-specific optimizations.

## Configuration Methods

### 1. Via Fixtures (Recommended for Initial Setup)

Edit `usaspending_api/llm/fixtures/assistants.yaml`:

```yaml
- model: llm.assistant
  pk: 1
  fields:
    name: filter-search
    ai_model: 1  # References AIModel pk
    system_prompt: 1  # References Prompts pk
    inference_config:
      temperature: 0.0
      topP: 1.0
      maxTokens: 5000
      stopSequences: []
```

Then load fixtures:
```bash
python manage.py load_llm_fixtures
```

### 2. Via Management Command (Recommended for Updates)

The `update_assistant` command provides comprehensive Assistant management.

#### List all Assistants and their configs:
```bash
# Short format (truncated system prompt)
python manage.py update_assistant --list

# Full format (complete system prompt)
python manage.py update_assistant --list-with-prompts
```

#### Update inference parameters:
```bash
# Update temperature
python manage.py update_assistant --name "filter-search" --temperature 0.5

# Update multiple parameters
python manage.py update_assistant \
  --name "filter-search" \
  --temperature 0.7 \
  --top-p 0.9 \
  --max-tokens 4096

# Update stop sequences
python manage.py update_assistant \
  --name "filter-search" \
  --stop-sequences "Human:,User:,\\n\\n"
```

#### Update with full JSON config:
```bash
python manage.py update_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.8, "topP": 0.95, "maxTokens": 8192, "stopSequences": ["Human:", "User:"]}'
```

#### Use model defaults for specific parameters:
```bash
# Let the model use its own default for maxTokens, but control temperature
python manage.py update_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.5, "topP": 0.9, "maxTokens": null, "stopSequences": []}'

# Let the model use all its defaults except temperature
python manage.py update_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.0, "topP": null, "maxTokens": null, "stopSequences": null}'
```

#### Clear inference config (revert to defaults):
```bash
python manage.py update_assistant --name "filter-search" --clear-inference-config
```

#### Update AI Model:
```bash
# By model ID
python manage.py update_assistant \
  --name "filter-search" \
  --model-id "anthropic.claude-sonnet-4-5-20250929-v1:0"

# By model name
python manage.py update_assistant \
  --name "filter-search" \
  --model-name "claude 4.5"
```

#### Update System Prompt:
```bash
# Use an existing prompt by ID
python manage.py update_assistant \
  --name "filter-search" \
  --system-prompt-id 2

# Create a new prompt
python manage.py update_assistant \
  --name "filter-search" \
  --new-system-prompt "You are a helpful assistant..."

# Combine multiple prompts
python manage.py update_assistant \
  --name "filter-search" \
  --system-prompt-id 2 \
  --new-system-prompt "Additional instructions..." \
  --combine-prompts

# Clear system prompt
python manage.py update_assistant \
  --name "filter-search" \
  --clear-system-prompt
```

#### Combined updates:
```bash
# Update model, prompt, and inference config together
python manage.py update_assistant \
  --name "filter-search" \
  --model-name "claude 4.5" \
  --temperature 0.5 \
  --max-tokens 5000 \
  --system-prompt-id 3
```

### 3. Via Django Shell

```python
from usaspending_api.llm.models.db_models import Assistant, AIModel, Prompts

# Get assistant
assistant = Assistant.objects.get(name="filter-search")

# Update inference config
assistant.inference_config = {
    "temperature": 0.5,
    "topP": 0.8,
    "maxTokens": 4096,
    "stopSequences": ["Human:", "\n\nUser:"],
}
assistant.save()

# Use model defaults for some parameters (set to None)
assistant.inference_config = {
    "temperature": 0.3,
    "topP": None,  # Use model's default
    "maxTokens": None,  # Use model's default
    "stopSequences": [],
}
assistant.save()

# Update AI model
model = AIModel.objects.get(name="claude 4.5")
assistant.ai_model = model
assistant.save()

# Update system prompt
prompt = Prompts.objects.get(name="filter-search-prompt")
assistant.system_prompt = prompt
assistant.save()

# View current config
print(f"Model: {assistant.ai_model.name}")
print(f"Inference Config: {assistant.inference_config}")
print(f"System Prompt: {assistant.system_prompt.text[:100]}...")
```

### 4. In Tests

#### Integration tests (with database):
```python
@pytest.mark.django_db
def test_custom_assistant_inference():
    ai_model = AIModel.objects.create(
        name="test model",
        model_id="test-id",
        provider="test"
    )
    
    prompt = Prompts.objects.create(
        name="test prompt",
        description="Test prompt",
        text="You are a test assistant."
    )
    
    assistant = Assistant.objects.create(
        name="test-assistant",
        ai_model=ai_model,
        system_prompt=prompt,
        inference_config={
            "temperature": 0.8,
            "topP": 0.9,
            "maxTokens": 512,
            "stopSequences": [],
        }
    )
    
    assert assistant.inference_config["temperature"] == 0.8
    assert assistant.ai_model.name == "test model"
```

#### Unit tests (with mocks):
```python
def test_with_mock_assistant():
    mock_assistant = Mock(spec=Assistant)
    mock_assistant.name = "test-assistant"
    mock_assistant.inference_config = {
        "temperature": 0.3,
        "topP": 0.5,
        "maxTokens": 1024,
        "stopSequences": ["Human:"],
    }
    
    assert mock_assistant.inference_config["temperature"] == 0.3
```

## Validations

The management command automatically validates all inference parameters using Pydantic models:

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
When using `--inference-config-json`, the command validates:
- All parameter types are correct
- All parameter values are within valid ranges
- JSON is properly formatted

## Troubleshooting

### Config not being used
1. Check that the Assistant has config: `python manage.py update_assistant --list`
2. Verify the correct Assistant is being used in your code
3. Ensure migration has been applied: `python manage.py migrate llm`
4. Create a new Request context to see config changes

### Validation errors
The command will reject invalid values before saving to the database:
- temperature and topP must be 0.0 - 1.0
- maxTokens must be a positive integer
- All types must match expected types (no strings for numeric values)

### Empty or null config values
- If `inference_config` is empty `{}`, the Assistant will use the default values shown above
- Individual parameters can be set to `null` to use the model's own defaults (parameter will be omitted from the inference request)
- This allows fine-grained control: you can specify some parameters explicitly while letting others use model defaults

### Assistant not found
- List all Assistants: `python manage.py update_assistant --list`
- Check the Assistant name matches exactly (case-sensitive)
- Ensure fixtures have been loaded: `python manage.py load_llm_fixtures`

## Architecture

### Assistant Model
The `Assistant` model combines:
- **name**: Unique identifier for the Assistant
- **ai_model**: Foreign key to `AIModel` (which model to use)
- **system_prompt**: Foreign key to `Prompts` (instructions for the model)
- **inference_config**: JSONField with inference parameters
