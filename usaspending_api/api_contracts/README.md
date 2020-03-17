# API Contracts

## For Everyone
All new API endpoint documentation must be in markdown that conforms to the [API Blueprint](https://apiblueprint.org/) syntax.


## For Developers
See [Tools for Testing and Mocking](tools-for-contracts.md)

To spin up a mock server, see the following instructions below:

1. run `npm install` at the root of `/API_CONTRACTS`
1. run `npm run mock`
1. This will spin up a mock implementation of the API at port 5000.

### Troubleshooting the Mock Server
1. If there is no Schema definition of the request object with an endpoint, you will have to exactly match the request object as defined.
1. To allow the API to respond to requests other than those that are hard-coded into the contract, add the following to the Request object definition of the Contract:
```markdown
    + Schema
        {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object"
        }
```

## Guidelines to Remember when Writing Contracts
1. One endpoint per markdown file
1. Endpoints that are different HTTP methods can share one markdown file
1. Markdown filenames should be in snake_case
1. Folder structure should mimic the URL and use "snake_case"
    - Mimic the URL path after `api/`
    - Currently, API Contracts are not being written for v1 endpoints
1. Don't include example values in the response objects
1. Escape all items represented as strings by JSON with backticks: "\`"
    - This avoids issues with special characters and helps with consistency
    - This includes all keys and string values

Follow this link to see an [API Contract template](template.md)
