FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC Autocomplete [/api/v2/autocomplete/psc/]

This endpoint is used by the Advanced Search page.

## POST

This route sends a request to the backend to retrieve product or service (PSC) codes and their descriptions based on a search string. This may be the 4-character PSC code or a description string.

+ Request (application/json)
    + Attributes (object)
        + `limit` (optional, number)
            + Default: 10
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array)
            + (object)
                + `product_or_service_code` (required, string)
                + `psc_description` (required, string)
