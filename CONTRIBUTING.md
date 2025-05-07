# Contributing to USAspending-api
Thanks for finding the Contributing file! We hope to foster a community around this project to drive improvements and hear how it is valuable to you.

Everything in this Github repository is a work-in-progress following the agile methodology of incrementally delivering value. This especially includes documentation. If you see any lacking areas, feel free to create a PR for the community.

# Submitting Changes

## Making a Branch
If you have write access to the Github repository, create a git branch from where you want to base your work - Typically, this will be `qat`

Otherwise, clone the repo and base your changes from the `qat` branch of your repo and submit a PR into the `qat` branch of this repo.

Try to follow these naming conventions so branches will be organized in a list, easily distinguished, and easily traced to the work involved.

In general, follow the form `<prefix>/[dev|gh]-###-short-description`

* `<prefix>` options are described below.
* `[DEV|GH]`choose "DEV" if the work follows a Jira ticket, "GH" if it is a Github issue.
* `###` is the associated Jira ticket or Github issue number (or the primary ID if multiple tickets or issues apply).
* `short-description` a few words to concisely summarize what it is.

| Prefix |Change Type|Examples|
|--------|-----------|--------|
| `ftr/` |_New_ stuff. New feature development, or new framework, utility, etc.|`ftr/dev-123-short-description`|
| `mod/` |Updates, refactoring, improvements, or modifications to an _existing_ feature/framework/code. <br/>_(If this is happening as part of new feature development, use `ftr/` instead.)_|`mod/dev-456-short-description`|
| `bug/` |Bug fixes that are not being hotfixed|`bug/gh-21-the-problem`|
| `fix/` |Hotfixes patching the current production code|`fix/dev-888-bug-description`|
| `doc/` |In app repos when only text files needs to change, usually markdown documentation|`doc/dev-555-what-the-doc-is`|


## Code Style
Use of these tools locally creates an excellent chance of passing our automated CI syntax checks.
* [flake8](https://flake8.pycqa.org/en/latest/)
    * Will be installed when following the steps in the [readme](README.md)
    * From repo root dir, run: `flake8`
    * If using a plugin in an IDE or code editor, ensure the configuration matches the `[flake8]` section in [setup.cfg](setup.cfg)
* [black](https://black.readthedocs.io/en/stable/)
    * Will be installed when following the steps in the [readme](README.md)
    * From repo root dir, run: `black .`
    * If using a plugin in an IDE or code editor, ensure the Black tool configuration matches the `[tool.black]` section in [pyproject.toml](pyproject.toml)

### Pre-commit Hooks
To assist developers it is possible to leverage pre-commit hooks to run several checks before git creates a commit. The Python dependency is included in the requirements files. To get started, run

    pre-commit install

Which will add itself to your local git repo config and begin to operate in the background. If you do not wish to participate in pre-commits, you can either uninstall pre-commit or commit your code with the --no-verify switch. Code checks will continue to occur in GitHub Actions, this is just a way to short circuit certain common errors that can be quickly and easily caught before wasting GitHub Action runners.

**IMPORTANT** If Black updates any files, the commit will fail and those files will be moved out of the git staging area (obviously, since they have been edited). You will need to re-add those files to staging (`git add` or however you do it using the GUI) before you re-commit your commit.

## Automated Tests
In conjunction with the above code syntax checks, it is recommended to run tests locally before pushing code and opening PRs
* [pytest](https://pytest.org/)
    * Will be installed when following the steps in the [readme](README.md)
    * Requires `DATABASE_URL` being set with a connection string to a working postgres database and `ES_HOSTNAME` to a functional Elasticsearch cluster
    * From repo root dir, run: `pytest`
        * There are many flags which can be used to help with the local testing process. Please read official documentation to understand the various flags. To reduce the output and stop at the first non-passing test, use this example:
            * `pytest --no-cov --sw --disable-warnings`
        * A directory or file path can also be provided to limit which tests are run. Example:
            * `pytest usaspending_api/awards/tests/integration/`

## Pull Requests
When opening a PR, the description will auto-populate with a template found [here](.github/pull_request_template.md)

### Stepping through the PR Template Requirements

> Some requirements _may not_ be applicable for a given PR.
>
> If one is not: please check the box, place "(N/A)" at the end of the line and include the reason in the bottom area. Clearly linking the comment to the requirement.
>
> Example: 2. [x] API documentation updated (N/A)

1. **Unit & integration tests updated**
    - Update existing automated unit/integration tests if any broke.
        - Verify the original test behavior is no longer correct **before** you modify any tests!
    - New functionality (new API, new management command, etc.) are expected to have tests in new test files located in either `tests/unit/` or `tests/integration'/` in the respective Django app.
    - For bug fixes, add new tests to verify the desired behavior and detect future regressions
2. **API documentation updated**
    - API Blueprint markdown files are located in `usaspending_api/api_contracts/`
    - If the API contract exists and the API endpoint definition was modified, the contract needs to be updated.
    - If a contract does not exist, it must be created alongside the new endpoint, the contract needs to be vetted for good design and the API endpoint behavior needs to match the contract.
3. **Necessary PR reviewers:**
    - Required: `Backend`
    - Several options: (Frontend|Operations|Domain Expert)
    - When a PR contains changes which should be validated by a representative of another team include a checkbox for that team.
    - Requires communication outside of GitHub to notify the reviewer(s)
4. **Matview impact assessment completed**
    - If anything dealing with the materialized view creation (JSON files, indexes, SQL generator script) is modified, write in the "Technical details" or a PR comment what the impact is, what the changes to operations are, and any concerns for the re-create.
5. **Frontend impact assessment completed**
    - If the PR creates or changes any API endpoint, a Frontend developer needs to be notified and allow them to work on a sister-PR before this PR is merged
6. **Data validation completed**
    - Perform basic data validation and include the results in the JIRA ticket
    - Using SQL or the API as appropriate to the PR changes
    - It's a good idea to reach out to domain experts or the PR reviewer to help with this requirement if the changes are significant or complex.
7. **Appropriate Operations ticket(s) created**
    - For any PRs which touch the database or require OPS involvement, create the correct type of JIRA ticket. Most likely it would be a `Data Change` as described [here](/Operations/Data%20Management/Production%20Data%20Change%20Process.md)
8. **Jira Ticket [DEV-0](https://federal-spending-transparency.atlassian.net/browse/DEV-0):**
    - (Link to this Pull-Request) In the Jira ticket, add a comment with link to this Github PR
    - (Performance evaluation)
        - Run a number of actual executions of the new API | Script | Download
            -  Use different permutations if appropriate and include the metrics in the Jira ticket comment.
    - (Before / After data comparison) Data validation results from #6 in a Jira ticket comment

**Bottom area:**
If any of the above required checkboxes are marked as `(N/A)`, then write down the explanation of why that requirement is not applicable in this PR. This is useful to critically think about each requirement and gives the author a chance to re-evaluate the `N/A`


# Final Notes

## License
Note: this codebase supports a Federal US Government [website](https://www.usaspending.gov).

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/legalcode).

All contributions to this project will be released under the [CC0 dedication](LICENSE). By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

## Additional Resources
- [USAspending.gov Community](https://usaspending-help.zendesk.com/hc/en-us/community/topics)
- [USAspending Release Notes](https://github.com/fedspendingtransparency/usaspending-website/wiki)
- [About Federal Spending Transparency](http://fedspendingtransparency.github.io/)
- [Our Amazing Frontend Repo](https://github.com/fedspendingtransparency/usaspending-website)
- [Full Dataset as PostgreSQL Dump](https://files.usaspending.gov/database_download/)
- [General GitHub Documentation](https://help.github.com/)
- [GitHub Pull Request Documentation](https://help.github.com/articles/creating-a-pull-request/)
