# API Contracts

Contract files are stored [here](contracts/) in this repo.

## For Everyone
All new API endpoint documentation must be in markdown that conforms to the [API Blueprint](https://apiblueprint.org/) syntax.


## For Developers
See [Tools for Testing and Mocking](tools-for-contracts.md)

## Guidelines to Remember when Writing Contracts
1. One and only one endpoint per markdown file
    - An endpoint can have multiple methods. These are stored in the same file unless the behavior is very disjointed.
1. Markdown filenames must be in snake_case
1. Folder structure must mimic the URL and use "snake_case"
    - Mimic the URL path after `api/`
    - Currently, API Contracts are not expected for v1 endpoints
1. Don't include example values in the request or response attributes
    - "Example" values are only allowed if they are for a required parameter.
1. Escape all items represented as strings by JSON with backticks: "\`"
    - This avoids issues with special characters and helps with consistency
    - This includes all keys and string values
1. Include the `Schema` block in the request to help with mock server tools
    - See examples
1. Include a properly-formatted and indented `Body` section for both a (Post) Request and Response
1. Use `dredd` or `aglio` to check the syntax of the API Contract.
1. Use `fixed-type` when defining list and object attributes
1. For consistency, use the plus sign, `+`, instead of a dash, `-`, in lists and object definitions

Follow these link to see example API Contracts:
- [[GET] /api/v2/this/is/your/<param_for_endpoint>/ (Example)](template_for_get.md)
- [[POST] /api/v5/query/widget/ (Example)](template_for_post.md)
