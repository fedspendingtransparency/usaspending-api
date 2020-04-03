FORMAT: 1A
HOST: https://api.usaspending.gov

# Glossary Autocomplete [/api/v2/autocomplete/glossary/]

This endpoint returns glossary autocomplete data for submitted text snippet.

## POST

List Autocomplete Glossary

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `search_text` (required, string)
            The text snippet that you are trying to autocomplete using a glossary term.
        + `limit` (optional, number)
            Maximum number to return
    + Body

            {
                "search_text": "Award"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[string], fixed-type)
        + `count` (required, number)
        + `search_text` (required, string)
        + `matched_terms` (required, array[GlossaryListing], fixed-type)

    + Body

            {
                "results": [
                    "Award",
                    "Award Amount",
                    "Award ID",
                    "Award Type",
                    "Awarding Agency",
                    "Awarding Office",
                    "Awarding Sub-Agency",
                    "Current Award Amount",
                    "Multiple Award Schedule (MAS)",
                    "Parent Award Identification (ID) Number"
                ],
                "search_text": "aW",
                "count": 10,
                "matched_terms": [
                    {
                        "term": "Award",
                        "slug": "award",
                        "data_act_term": null,
                        "plain": "Money the federal government has promised to pay a recipient. Funding may be awarded to a company, organization, government entity (i.e., state, local, tribal, federal, or foreign), or individual. It may be obligated (promised) in the form of a contract, grant, loan, insurance, direct payment, etc.",
                        "official": null,
                        "resources": null
                    },
                    {
                        "term": "Award Amount",
                        "slug": "award-amount",
                        "data_act_term": "Amount of Award",
                        "plain": "The amount that the federal government has promised to pay (obligated) a recipient, because it has signed a contract, awarded a grant, etc. ",
                        "official": "The cumulative amount obligated by the Federal Government for an award, which is calculated by USAspending.gov.\n\nFor procurement and financial assistance awards except loans, this is the sum of Federal Action Obligations.\n\nFor loans or loan guarantees, this is the Original Subsidy Cost.",
                        "resources": "See also:\n\n- [Federal Action Obligation](?glossary=federal-action-obligation)\n- [Subsidy Cost](?glossary=subsidy-cost)\n- [Current Total Value of Award](?glossary=current-award-amount)"
                    }
                ]
            }


# Data Structures

## GlossaryListing (object)
+ `term` (required, string)
+ `slug` (required, string)
+ `data_act_term` (required, string, nullable)
+ `plain` (required, string)
+ `official` (required, string, nullable)
+ `resources` (required, string, nullable)
