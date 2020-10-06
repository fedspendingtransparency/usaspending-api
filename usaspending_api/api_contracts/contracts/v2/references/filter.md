FORMAT: 1A
HOST: https://api.usaspending.gov

# Generated Filter Hash for URL [/api/v2/references/filter/]

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data. Primarily used when user shares or saves a url from an Advanced Search results page.

## POST

Generates a hash for URL, based on selected filter criteria.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
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
                "filters": {
                    "keyword": {},
                    "timePeriodType": "fy",
                    "timePeriodFY": [
                        "2020"
                    ],
                    "timePeriodStart": null,
                    "timePeriodEnd": null,
                    "selectedLocations": {
                        "USA_AZ": {
                            "identifier": "USA_AZ",
                            "filter": {
                                "country": "USA",
                                "state": "AZ"
                            },
                            "display": {
                                "entity": "State",
                                "standalone": "Arizona",
                                "title": "Arizona"
                            }
                        }
                    },
                    "locationDomesticForeign": "all",
                    "selectedFundingAgencies": {},
                    "selectedAwardingAgencies": {
                        "806_toptier": {
                            "id": 806,
                            "toptier_flag": true,
                            "toptier_agency": {
                                "id": 68,
                                "toptier_code": "075",
                                "abbreviation": "HHS",
                                "name": "Department of Health and Human Services"
                            },
                            "subtier_agency": {
                                "abbreviation": "HHS",
                                "name": "Department of Health and Human Services"
                            },
                            "agencyType": "toptier"
                        }
                    },
                    "selectedRecipients": [],
                    "recipientDomesticForeign": "all",
                    "recipientType": [],
                    "selectedRecipientLocations": {},
                    "awardType": [
                        "IDV_D",
                        "IDV_B_A",
                        "IDV_E",
                        "IDV_B_B",
                        "IDV_B_C",
                        "07",
                        "08",
                        "IDV_A",
                        "IDV_B",
                        "IDV_C"
                    ],
                    "selectedAwardIDs": {},
                    "awardAmounts": {
                        "range-1": [
                            1000000,
                            25000000
                        ]
                    },
                    "selectedCFDA": {},
                    "selectedNAICS": {},
                    "selectedPSC": {},
                    "pricingType": [],
                    "setAside": [],
                    "extentCompeted": [],
                    "federalAccounts": {},
                    "treasuryAccounts": {}
                },
                "version": "2019-07-26"
            }

+ Response 200 (application/json)
    + Attributes
        + `hash` (required, string)
    + Body

            {
                "hash":"3ef42b40bda7dd723e1ffcd542a7d9c7"
            }