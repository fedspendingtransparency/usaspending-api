# Managing AI Assistant Configurations

This guide explains how to configure and manage AI Assistants, including their inference parameters, system prompts, and AI models.

## Overview

AI Assistants are configured entities that combine an AI model, system prompt, and inference configurations. Each Assistant can be independently configured and updated using Django management commands.

**Important Notes:**
- Inference configurations are managed on a per-Assistant basis
- Updating configurations mid-conversation will not result in changes; you must create a new Request context
- Changes to an Assistant affect all users of that Assistant (global changes)

## Assistant Identity and Activation

Assistant names are not unique. Multiple inactive Assistants may share the same name, but at most one Assistant with a given name may be active.

- `--name NAME` selects the **active** Assistant with that name. Raises an error if no active Assistant is found.
- `--pk PK` selects the exact Assistant row, including inactive Assistants.
- Use `--pk` when updating an inactive Assistant or choosing among multiple Assistants with the same name.
- Activating an Assistant automatically deactivates any other active Assistant with the same name.

Creating an Assistant requires an existing `AIModel`. Supply either `--model-id` or `--model-name`; when both are supplied, `--model-id` takes precedence. AI models referenced by Assistants are protected from deletion.

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
  "temperature": 0.0,
  "topP": 1.0,
  "maxTokens": 5000,
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
    is_active: true # Whether the Assistant is the one currently in use. Set as `false` to deactivate.
    description: "Placeholder description"
```

Then load fixtures:
```bash
python manage.py load_llm_fixtures
```

### 2. Via Management Command (Recommended for Updates)

The `manage_llm_assistant` command lists, creates, activates, deactivates, and updates Assistants. The command uses logging for user-facing output.

#### List all Assistants and their configs:
```bash
# Short format (truncated system prompt)
python manage.py manage_llm_assistant --list

# Full format (complete system prompt)
python manage.py manage_llm_assistant --list-with-prompts
```

`--list` and `--list-with-prompts` are mutually exclusive. Either list option must be used by itself and cannot be combined with create or update options.

For example, this is invalid:

```bash
python manage.py manage_llm_assistant --list --name "filter-search"
```

#### Manage active state:
```bash
# Re-activate/update the currently active Assistant selected by name.
# This requires an active Assistant with this name to already exist.
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --is-active

# Deactivate a specific Assistant selected by primary key
python manage.py manage_llm_assistant \
  --pk 12 \
  --is-inactive
```

`--is-active` and `--is-inactive` cannot be supplied together. Activating an Assistant automatically deactivates any other active Assistant with the same name.

#### Update inference parameters:
```bash
# Update temperature
python manage.py manage_llm_assistant --name "filter-search" --temperature 0.5

# Update multiple parameters
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --temperature 0.7 \
  --top-p 0.9 \
  --max-tokens 4096

# Update stop sequences; repeat the option to provide multiple values for the list.
# Commas inside an individual stop sequence are preserved.
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --stop-sequences "Human:,User:" \
  --stop-sequences $'\n\n'
```

#### Update with JSON config:

The JSON object may contain all or only some inference fields. Supplied fields are merged with the current configuration; fields not already configured use the Assistant's deterministic defaults. Use `null` to remove a specific field from the request and allow the Bedrock model's defaults.

```bash
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.8, "topP": 0.95, "maxTokens": 8192, "stopSequences": ["Human:", "User:"]}'
```

#### Use model defaults for specific parameters:
```bash
# Let the model use its own default for maxTokens, but control temperature
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.5, "topP": 0.9, "maxTokens": null, "stopSequences": []}'

# Let the model use all its defaults except temperature
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --inference-config-json '{"temperature": 0.0, "topP": null, "maxTokens": null, "stopSequences": null}'
```

#### Clear inference config (revert to defaults set in the Assistant's code where instantiated):
```bash
python manage.py manage_llm_assistant --name "filter-search" --clear-inference-config
```

#### Update AI Model:
```bash
# By model ID
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --model-id "anthropic.claude-sonnet-4-5-20250929-v1:0"

# By model name
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --model-name "claude 4.5"
```

#### Update System Prompt:
```bash
# Use an existing prompt by ID
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --system-prompt-id 2

# Create a new prompt
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --new-system-prompt "You are a helpful assistant..." \
  --new-prompt-name "Helpful prompt"

# Combine multiple prompts
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --system-prompt-id 2 \
  --new-system-prompt "Additional instructions..." \
  --combine-prompts \
  --new-prompt-name "Combined system prompt with additional instructions"

# Clear system prompt
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --clear-system-prompt
```

To start from a blank prompt instead of combining the Assistant's current prompt, combine `--clear-system-prompt` with `--new-system-prompt` and/or `--system-prompt-id` and `--combine-prompts`. Clearing occurs before the new prompt is constructed; it is not a request to leave the Assistant without a prompt when other prompt flags are supplied. Newly created prompts must contain non-empty text.

`--new-prompt-name` applies only when the command creates a prompt through `--new-system-prompt` or `--combine-prompts`; it cannot rename an existing prompt. Prompt names must be unique, non-empty, and no longer than 100 characters.

#### Combined updates:
```bash
# Update model, prompt, and inference config together
python manage.py manage_llm_assistant \
  --name "filter-search" \
  --model-name "claude 4.5" \
  --temperature 0.5 \
  --max-tokens 5000 \
  --system-prompt-id 3
```

#### Create a new AI Assistant:
```bash
python manage.py manage_llm_assistant \
  --create-new \
  --name "filter-search" \
  --model-name "titan" \
  --inference-config-json '{"temperature": null, "topP": null, "maxTokens": null, "stopSequences": null}' \
  --system-prompt-id 2 \
  --is-active \
  --description "AI Assistant that uses Amazon Titan with its own default inference configs."
```

### 3. Via Django Shell

```python
from usaspending_api.llm.models.db_models import Assistant, AIModel, Prompts

# Get the active assistant by name
assistant = Assistant.objects.get(name="filter-search", is_active=True)

# Get a specific assistant, including an inactive assistant
assistant = Assistant.objects.get(pk=12)

# Update inference config
assistant.inference_config = {
    "temperature": 0.5,
    "topP": 0.8,
    "maxTokens": 4096,
    "stopSequences": ["Human:", "\n\nUser:"],
}
assistant.save(update_fields=["inference_config"])

# Use model defaults for some parameters (set to None)
assistant.inference_config = {
    "temperature": 0.3,
    "topP": None,  # Use model's default
    "maxTokens": None,  # Use model's default
    "stopSequences": [],
}
assistant.save(update_fields=["inference_config"])

# Update AI model
model = AIModel.objects.get(name="claude 4.5")
assistant.ai_model = model
assistant.save(update_fields=["ai_model"])

# Update system prompt
prompt = Prompts.objects.get(name="filter-search-prompt")
assistant.system_prompt = prompt
assistant.save(update_fields=["system_prompt"])

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
        },
        is_active=True,
        description="Test Assistant"
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
    mock_assistant.is_active = True
    mock_assistant.description = "Mock Assistant"
    
    assert mock_assistant.inference_config["temperature"] == 0.3
```

## Validations

The management command automatically validates all inference parameters using Pydantic models:

### Temperature
- **Range**: 0.0 to 1.0 (inclusive)
- **Type**: Float or integer
- Invalid values are reported as `Invalid inference config: ...` before saving.

### Top P
- **Range**: 0.0 to 1.0 (inclusive)
- **Type**: Float or integer
- Invalid values are reported as `Invalid inference config: ...` before saving.

### Max Tokens
- **Range**: Must be positive (> 0)
- **Type**: Integer only
- Invalid values are reported as `Invalid inference config: ...` before saving.

### Stop Sequences
- **Type**: List of strings
- **Format**: Repeat `--stop-sequences` for multiple values; commas inside an individual value are preserved
- **Example**: `--stop-sequences "Human:,User:" --stop-sequences "Assistant:"`
- **Empty values**: Empty stop sequences are rejected
- **Newlines**: Use shell-specific expansion such as `$'\n\n'` when an actual newline is required; the command does not decode a literal `\n` sequence

### JSON Config
When using `--inference-config-json`, the command validates:
- The input is a JSON object
- All parameter types are correct
- All parameter values are within valid ranges
- Unknown configuration keys are rejected
- JSON is properly formatted
- Individual inference flags cannot be combined with the JSON option
- `--clear-inference-config` cannot be combined with other inference options

## Troubleshooting

### Config not being used
1. Check that the Assistant has config: `python manage.py manage_llm_assistant --list`
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
- List all Assistants: `python manage.py manage_llm_assistant --list`
- Check the Assistant name matches exactly (case-sensitive)
- Ensure fixtures have been loaded: `python manage.py load_llm_fixtures`

## Architecture

### Assistant Model
The `Assistant` model combines:
- **name**: Identifier for the collection of Assistants used for the same purpose.
- **ai_model**: Foreign key to `AIModel` (which model to use).
- **system_prompt**: Foreign key to `Prompts` (instructions for the model).
- **inference_config**: JSONField with inference parameters.
- **is_active**: The active/inactive state of the Assistant. Only one `name/is_active=True` combo can exist per name (Unique Constraint).
- **description**: Optional text describing the setup/purpose of the Assistant. It defaults to an empty string.
