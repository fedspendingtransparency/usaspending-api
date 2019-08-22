FORMAT: 1A
HOST: https://api.usaspending.gov

# Generated Filter Hash for URL [/api/v1/references/filter/]

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

Generates a hash for URL, based on selected filter criteria.

+ Request (application/json)
    + Attributes (object)
        + `filter` (optional, object)
            + `filters` (optional, object)
                + `awardAmounts` (required, object)
                + `awardType` (required, array[string])
                + `extentCompeted` (required, array[string])
                + `keyword` (required, object)
                + `locationDomesticForeign` (required, string)
                + `pricingType` (required, array[string])
                + `recipientDomesticForeign` (required, string)
                + `recipientType` (required, array[string])
                + `selectedAwardIDs` (required, object)
                + `selectedAwardingAgencies` (required, object)
                + `selectedCFDA` (required, object)
                + `selectedFundingAgencies` (required, object)
                + `selectedLocations` (required, object)
                + `selectedNAICS` (required, object)
                + `selectedPSC` (required, object)
                + `selectedRecipientLocations` (required, object)
                + `selectedRecipients` (required, array[string])
                + `setAside` (required, array[string])
                + `timePeriodEnd` (required, string, nullable)
                + `timePeriodFY` (required, array[string])
                + `timePeriodStart` (required, string, nullable)
                + `timePeriodType` (required, string)
            + `version` (optional, string)

+ Response 200 (application/json)
    + Attributes
        + `hash` : `5703c297b902ac3b76088c5c275b53f9` (required, string)
