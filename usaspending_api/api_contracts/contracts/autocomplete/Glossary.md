## Glossary Autocomplete [/api/v2/autocomplete/glossary/]

This endpoint returns glossary autocomplete data for submitted text snippet.


+ Parameters
    + search_text: `ab` (required, string)
        The text snippet that you are trying to autocomplete using a glossary term.
    + limit: `10` (optional, integer)
        Maximum number to return

### List Autocomplete Glossary [POST]

+ Request (application/json)

    + Attributes

        + search_text: `ab` (required, string)
        The text snippet that you are trying to autocomplete using a glossary term.
        + limit: `10` (optional, number)
        Maximum number to return
        
+ Response 200 (application/json)
    + Attributes (GlossaryAutocompleteObject)

# Data Structures

## GlossaryListing (object)
+ term: Acquisition of Assets (required, string)
+ slug: acquisition-of-assets (required, string)
+ data_act_term: Acquisition of Assets (required, string)
+ plain: This major object class includes lorem ipsum (required, string)
+ official : This major object class covers object classes 31.0 through 33.0 lorem ipsum (required, string)
+ resources: Learn More: Circular No. A-11 (required, string, nullable)

## GlossaryAutocompleteObject (object)
+ results: (required, array[string])
+ count: 4 (required, number)
+ search_text: ab (required, string)
+ matched_terms:(required, array[GlossaryListing])