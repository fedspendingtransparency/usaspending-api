# Contributing to USAspending-api
Great! We hope to foster a community around this project to drive improvement and hear how it is valuable to you.

# Submitting Changes

## Code Style
Use these tools to have an excellent chance of passing our automated CI tests
- [flake8](http://flake8.pycqa.org/en/latest/index.html#)
  - Will be installed when following the steps in the [readme](README.md)
  - from repo root dir: `flake8`
- [black](https://github.com/python/black)
  - See instructions for installing black and integrating with your develop env. Currently blakc requires a newer version of Python than the usaspending-api application.
  - from repo root dir: `black .`

## Pull Requests
When opening a PR to improve or fix something, the description will auto-populate with a template found [here](.github/pull_request_template.md)

### Stepping through the PR Template Requirements

> Some requirements _may not_ be applicable for a given PR.
>
> If one is not: please check the box, place "(N/A)" at the end of the line, and include the reason in the bottom area. Clearly linking the comment to the requirement.
>
> Example: 2. [x] API documentation updated (N/A)

1. **Unit & integration tests updated**
    - Update existing automated unit/integration tests if any broke due to code changes
        - Verify the original test behavior is no longer correct **before** you modify any tests!
    - New functionality (new API, new management command, etc.) are expected to have tests in new test files located in either `tests/unit/` or `tests/integration'/` in the Django app.
    - For bug fixes, add new tests to account for the desired behavior and detect future regressions
2. **API documentation updated**
    - Markdown files are located in `usaspending_api/api_docs/`
    - Must be completed when any API call is modified
3. **Necessary PR reviewers:**
    - Required: `Backend`
    - Several options: (Frontend|Operations|Domain Expert)
    - When a PR contains changes which should be validated by a representative of another team include a checkbox for that team.
    - Requires communication outside of GitHub to notify the reviewer(s)
4. **Matview impact assessment completed**
    - When anything dealing with the matview creation (JSON files, indexes, SQL generator script) write in the "Technical details" or a PR comment what the impact is, what the changes to operations are, and any concerns for the re-create.
5. **Frontend impact assessment completed**
    - If the PR creates or changes any API endpoint, Front-end needs to be notified and allow them to work on a sister-PR before this PR is merged
6. **Data validation completed**
    - Perform basic data validation and include the results in the JIRA ticket
    - Using SQL or the API as appropriate to the PR changes
    - It's a good idea to reach out to domain experts or the PR reviewer to help with this requirement if the changes are significant or complex.
7. **Appropriate Operations ticket(s) created**
    - For any PRs which touch the database or require OPS involvement, create the correct type of JIRA ticket. Most likely it would be a `Data Change`  as described [here](/Operations/Data%20Management/Production%20Data%20Change%20Process.md)
8. **Jira Ticket [DEV-0](https://federal-spending-transparency.atlassian.net/browse/DEV-0):**
    - (Link to this Pull-Request) In the Jira ticket, add a comment with link to this Github PR
    - (Performance evaluation)
        - Run a number of actual executions of the new API | Script | Download
            -  Use different permutations if appropriate, and include the metrics in the Jira ticket comment.
    - (Before / After data comparison) Data validation results from #6 in a Jira ticket comment

**Bottom area:**
If any of the above required checkboxes are marked as `(N/A)`, then write down the explaination of why that requirement is not applicable in this PR. This is useful to critically think about each requirement and gives the author a chance to re-evaluate the `N/A`


# Final Notes

## License
Note: this codebase supports a Federal US Government [website](https://www.usaspending.gov).

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/legalcode).

All contributions to this project will be released under the [CC0 dedication](LICENSE). By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.