# API Contracts

## For Everyone
All new API endpoint documentation must be in markdown that conforms to the [API Blueprint](https://apiblueprint.org/) syntax.


## For Developers
See [Tools for Testing and Mocking](tools-for-contracts.md)

## Guidelines to Remember when Writing Contracts
1. Markdown filenames should be in snake_case
1. Endpoints that are different HTTP methods can share one markdown file
1. Folder structure should mimic the URL and use "snake_case"
    - Currently, API Contracts are not being written for v1 endpoints
1. Don't include example values in the response objects
1. Escape all items represented as strings by JSON with backticks: "\`"
    - This avoids issues with special characters and helps with consistency
    - This includes all keys and string values

Follow this link to see an [API Contract template](template.md)
