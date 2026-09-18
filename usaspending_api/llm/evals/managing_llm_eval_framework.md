# Managing the LLM Evaluation Framework

## Purpose

This eval framework provides a repeatable way to compare an assistant's observed behavior against approved ground truth.

For the current Filter Search assistant, evaluation covers two dimensions:

1. **Tool behavior**
   - Which tools were called?
   - Were they called in the expected order?
   - Did the assistant call the expected tools in the expected order?

2. **Final filter output**
   - What filter payload was passed to `execute_filter`?
   - Does that payload contain the expected filter values?

The framework uses deterministic comparisons.
It does not currently use an LLM judge or Pydantic Evals, but incorporating those could be a natural next step.

## High-Level Architecture

```text
run_llm_eval
    -> load config.json
    -> select approved/case/tag-filtered cases
    -> execute selected cases through FilterSearchAssistant
    -> read persisted ToolUse records
    -> compare expected tools and filters
    -> calculate scores
    -> output text, JSON, or XLSX report
```

## Directory Structure

```text
usaspending_api/llm/
├── evals/
│   ├── base.py
│   ├── exceptions.py
│   ├── loader.py
│   ├── matchers.py
│   ├── models.py
│   ├── registry.py
│   ├── reporting.py
│   ├── data/
│   │   └── config.json
│   └── assistants/
│       └── filter_search.py
├── management/
│   └── commands/
│       └── run_llm_eval.py
└── tests/
    └── evals/
        └── test_reporting.py
```

## Runtime `config.json` source of truth

The runtime source of truth is the manually maintained JSON file:

```text
usaspending_api/llm/evals/data/config.json
```

`config.json` should be reviewed and updated through the normal source-control process.
Changes should be visible in code review so the team can see:

- which cases were added or removed;
- which expected tools changed;
- which expected filters changed;
- which cases became approved;
- which tags or stakeholder notes changed.

## Runtime `config.json` Format

The manually maintained `config.json` is consumed directly by `llm/evals/loader.py`.

Example:

```json
[
  {
    "id": 1,
    "query": "How much did Clark Construction receive in contracts for FY25?",
    "expected_output": {
      "selectedRecipients": [
        "Clark Construction"
      ],
      "timePeriodType": "fy",
      "timePeriodFY": [
        "2025"
      ],
      "awardType": [
        "Contracts"
      ]
    },
    "expected_tools": [
      "lookup_recipient",
      "execute_filter"
    ],
    "tags": [
      "multi_filter",
      "temporal"
    ],
    "notes": "Stakeholder context",
    "approved": true,
    "sme_validation_notes": "Reviewed by SME"
  }
]
```

The runtime field names should match the actual Pydantic `Filters` model and the actual `AIToolDescription.name` values.

For example, the current backend uses fields such as:

```text
selectedRecipients
timePeriodType
timePeriodFY
time_period
awardType
selectedAwardIDs
selectedLocations
...etc.
```

The exact expected values should come from reviewed behavior rather than assumptions about capitalization or aliases.

## Running Evaluations

Run all approved cases:

```sh
python manage.py run_llm_eval \
  --assistant filter_search
```

Run selected case IDs:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --case 1 \
  --case 3
```

Run cases matching a tag:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --tag temporal
```

Include cases that are not approved:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --include-unapproved
```

Require a minimum score:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --fail-under 0.90
```

The score is calculated only from cases that actually run.
Cases excluded because of approval, case, or tag filters appear separately as `NOT RUN` in exported reports.

## How Filter Search Evaluation Works

For each selected case, the concrete evaluator:

1. Retrieves the active Assistant configuration.
2. Creates a Session.
3. Instantiates the Assistant with the endpoint’s tools.
4. Executes `assistant.search(query)`.
5. Reads the persisted `ToolUse` records.
6. Compares the actual tool sequence with `expected_tools`.
7. Finds the successful `execute_filter` ToolUse.
8. Canonicalizes its input through the production filter-building logic.
9. Compares the resulting filters with `expected_output`.
10. Produces an `EvalResult`.

The initial case score is:

```text
tool behavior correct + filter output correct = 1.0

tool behavior correct + filter output incorrect = 0.5

tool behavior incorrect + filter output correct = 0.5

tool behavior incorrect + filter output incorrect = 0.0
```

The aggregate score is the average (`fmean`) of executed case scores.

## Report Output

### Text output

Text output is intended for terminal logs and quick local review.

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --format text
```

### JSON output

JSON output includes:

```text
summary
    Overall run metrics.

tags
    Per-tag aggregation.

cases
    Executed and NOT RUN case rows.

results
    Compatibility alias for cases.
```

Export JSON to a file:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --format json \
  --output /tmp/filter-search-report.json
```

### Excel output

Excel output is intended for stakeholder review and sharing:

```sh
python manage.py run_llm_eval \
  --assistant filter_search \
  --format xlsx \
  --output /tmp/filter-search-report.xlsx
```

The workbook has three sheets.

#### Summary

Contains:

```text
assistant
dataset
case_count
total_case_count
unrun_count
passed_count
failed_count
score
fail_under
passed
generated_at
```

`case_count` means cases executed. `total_case_count` includes cases represented as `NOT RUN`.

#### Tags

Contains one row per tag:

```text
tag
total_cases
run_cases
not_run_cases
passed_count
failed_count
average_score
pass_rate
```

A case can contribute to multiple tag rows. Cases without tags are grouped under `(untagged)`.

#### Evaluation

Contains one row for every loaded case:

```text
case_name
case_id
query
status
score
tool_passed
tool_score
tool_message
expected_tools
actual_tools
output_passed
output_score
output_message
expected_output
actual_output
case_metadata
execution_metadata
assistant
assistant_id
ai_model_id
system_prompt_id
inference_config_temp
inference_config_top_p
inference_config_max_tokens
inference_config_stop_sequences
```

Executed rows have `PASS` or `FAIL` status. Excluded rows have `NOT RUN` status.

For excluded rows:

```text
expected_tools and expected_output
    remain populated from ground truth

actual_tools and actual_output
    are N/A

scores and pass flags
    are N/A

metadata that requires execution
    is N/A
```

The row also contains a reason in `tool_message` and `output_message`, such as:

```text
case is not approved
case was not selected
case did not match the selected tags for this run
```

## Assistant Metadata

For executed cases, the Evaluation sheet includes configuration metadata captured from the active Assistant record:

```text
assistant
    Assistant database name, such as filter-search.

assistant_id
    Database ID, allowing duplicate assistant names to be distinguished.

ai_model_id
    Configured Bedrock model ID.

system_prompt_id
    Database ID of the configured system prompt.

inference_config_temp
    Configured temperature.

inference_config_top_p
    Configured top-P value.

inference_config_max_tokens
    Configured maximum token count.

inference_config_stop_sequences
    Configured stop sequences.
```

These values help explain why a result may differ between evaluations even when the ground-truth query is unchanged.

## Ground Truth Maintenance

`config.json` is the source of truth for ongoing evaluation maintenance.
It is manually created and then updated directly as cases, expected behavior, tags, and approval decisions evolve.

Recommended ongoing workflow:

```text
1. Add or update cases directly in config.json.
2. Review the JSON change in source control.
3. Confirm expected tools and backend filter fields are current (align with output hash/filter values).
4. Set approved=false while a case is being reviewed.
5. Set approved=true after SME/team approval.
6. Run `run_llm_eval` against the approved cases.
7. Distribute the generated XLSX report to stakeholders.
8. Retain reports over time to show accuracy evolution.
```

When adding a new case:

1. Add a unique `id`.
2. Add the natural-language `query`.
3. Add expected backend filter values in `expected_output`.
4. Add expected runtime tool names in `expected_tools`.
5. Confirm the fields match the Pydantic `Filters` model or actual tool names.
6. Add relevant tags.
7. Set `approved` to `false` until SME review is complete.
8. Set `approved` to `true` only after the expected behavior is accepted.

The report is the stakeholder-facing output.
It should not be edited and fed back into `config.json` automatically.
Any ground-truth change should be made intentionally in the JSON source.

## Ground Truth Values and Normalization

The framework compares expected filter values to the canonical filter payload passed to `execute_filter`.

Do not assume that a natural-language value is the exact runtime value. For example:

```text
Clark Construction
CLARK CONSTRUCTION
Clark Construction, Inc.
```

may represent different values in the recipient index.
Expected values should be based on the actual reviewed assistant execution and the values accepted by the filter model.

Similarly, locations, agencies, award IDs, and code filters may require structured values rather than simple strings.

## Bedrock and Side Effects

`run_llm_eval` can call Bedrock when it invokes the real LLM assistant. A live evaluation can also:

- create Session records;
- create Message records;
- create ToolUse records;
- call OpenSearch-backed tools;
- create or reuse FilterHash records.

Use mocked assistant/Bedrock behavior for unit tests.

## Troubleshooting

### No cases available

Check:

- `approved` values;
- `--include-unapproved`;
- `--case` values;
- `--tag` values;
- `LLM_EVAL_DATASET_DIRECTORY`;
- whether `config.json` contains the intended reviewed cases; 
- whether a manually generated candidate from Excel was merged into `config.json`.

### XLSX report contains N/A rows

`NOT RUN` rows are intentional. They represent validated source cases that were excluded by the current command filters or approval settings. They retain expected values but have no actual assistant execution data.
