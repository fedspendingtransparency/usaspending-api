FORMAT: 1A
HOST: https://api.usaspending.gov

# Restore Filters From URL Hash [/api/v1/references/hash/]

Restores selected filter criteria, based on URL hash. Supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

Restore Filters From URL Hash Data 

+ Request (application/json)
    + Attributes (object)
        + `hash` : `0e7d2ce3bb0885ac877872bb44053a84` (required, string)
    + Schema
        {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object"
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
                        "awardAmounts": {},
                        "awardType": [],
                        "extentCompeted": [],
                        "keyword": {},
                        "locationDomesticForeign": "all",
                        "pricingType": [],
                        "recipientDomesticForeign": "all",
                        "recipientType": [],
                        "selectedAwardIDs": {},
                        "selectedAwardingAgencies": {},
                        "selectedCFDA": {},
                        "selectedFundingAgencies": {},
                        "selectedLocations": {},
                        "selectedNAICS": {},
                        "selectedPSC": {},
                        "naics_codes": {
                            checked: ["11"],
                            excluded: []
                        }
                        "selectedRecipientLocations": {},
                        "selectedRecipients": [],
                        "setAside": [],
                        "timePeriodEnd": null,
                        "timePeriodFY": ["2019"],
                        "timePeriodStart": null,
                        "timePeriodType": "fy"
                    },
                    "version": "2017-11-21"
                }
            }
