# API Contracts

## For Everyone
All new API endpoint documentation must be in markdown that conforms to the [API Blueprint](https://apiblueprint.org/) syntax.


## For Developers
See [Tools for Testing and Mocking](tools-for-contracts.md)

## Guidelines to Remember when Writing Contracts
1. One endpoint per markdown file
1. Markdown filenames should be in "CamelCase"
1. Folder structure should mimic the URL and use "snake_case"
    - Don't include "api/v2" in the folder structure
    - Currently, API Contracts are not being written for v1 endpoints
1. Don't include example values in the response objects
1. Since underscores can cause parsing troubles, escape all example strings with backticks: "\`"
1. If any key contains an underscore, escape it with backticks and all other sister keys in that list/object/enum
