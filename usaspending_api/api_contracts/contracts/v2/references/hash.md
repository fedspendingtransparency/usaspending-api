FORMAT: 1A
HOST: https://api.usaspending.gov

# Restore Filters From URL Hash [/api/v2/references/hash/]

Restores selected filter criteria, based on URL hash. Supports the advanced search page and allow for complex filtering for specific subsets of spending data. Primarily used when user shares or saves a url from an Advanced Search results page.

## POST

Restore Filters From URL Hash Data

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `hash` (required, string)
            + Default: `5703c297b902ac3b76088c5c275b53f9` (retruns  default filters)

    + Body

            {
                "hash": "5703c297b902ac3b76088c5c275b53f9"
            }

+ Response 200 (application/json)
    + Attributes
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

    + Body

            {
                "filter": {
                    "filters": {
                        "keyword": {},
                        "setAside": [],
                        "awardType": [],
                        "pricingType": [],
                        "selectedPSC": {},
                        "awardAmounts": {},
                        "selectedCFDA": {},
                        "timePeriodFY": [
                            "2019"
                        ],
                        "recipientType": [],
                        "selectedNAICS": {},
                        "timePeriodEnd": null,
                        "extentCompeted": [],
                        "timePeriodType": "fy",
                        "timePeriodStart": null,
                        "selectedAwardIDs": {},
                        "selectedLocations": {},
                        "selectedRecipients": [],
                        "locationDomesticForeign": "all",
                        "selectedFundingAgencies": {},
                        "recipientDomesticForeign": "all",
                        "selectedAwardingAgencies": {},
                        "selectedRecipientLocations": {}
                    },
                    "version": "2017-11-21"
                }
            }
