FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Subaward Grouped

## POST

This endpoint takes award filters and returns a list containing filtered award ids, the number of subawards in each award, the total amount of obligations of all the subawards, and the award unique id.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
        + `limit` (optional, number)
            How many results are returned
        + `page` (optional, number)
            The page number that is currently returned.
        + `sort`: Any field within the result can be sort (optional, enum[string])
            The field on which to order `results` in the response.
            + Default: `award_id`
            + Members
                + `award_generated_internal_id`
                + `award_id`
                + `subaward_count`
                + `subaward_obligation`
        + `order` (optional, enum[string])
            Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
            + Default: `desc`
            + Members
                + `asc`
                + `desc`
    + Body

            {
                "limit": 10,
                "page": 1,
                "filters": {
                    "award_type_codes": ["A", "B", "C"],
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2019-09-30"}]
                },
                "sort": "award_id",
                "order": "desc"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `limit` (required, number)
        + `results` (required, array[SpendingBySubawardGroupedResult], fixed-type)
        + `page_metadata` (PageMetadataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    
    + Body

            {
                "limit": 10,
                "results": [
                    {
                        "award_id": "0007",
                        "subaward_count": 23,
                        "award_generated_internal_id": "CONT_AWD_0007_9700_W31P4Q09A0021_9700",
                        "subaward_obligation": 6942858.2
                    },
                    {
                        "award_id": "1333LB19F00000306",
                        "subaward_count": 6,
                        "award_generated_internal_id": "CONT_AWD_1333LB19F00000306_1323_GS35F110DA_4732",
                        "subaward_obligation": 1158693.0
                    },
                    {
                        "award_id": "91003119F0003",
                        "subaward_count": 50,
                        "award_generated_internal_id": "CONT_AWD_91003119F0003_9100_HHSN316201200002W_7529",
                        "subaward_obligation": 16349055.0
                    },
                    {
                        "award_id": "FA852818F0030",
                        "subaward_count": 5,
                        "award_generated_internal_id": "CONT_AWD_FA852818F0030_9700_FA852816D0009_9700",
                        "subaward_obligation": 482721.0
                    },
                    {
                        "award_id": "FA865118F1016",
                        "subaward_count": 3,
                        "award_generated_internal_id": "CONT_AWD_FA865118F1016_9700_FA865116D0314_9700",
                        "subaward_obligation": 258711.38
                    },
                    {
                        "award_id": "HSHQDC17J00370",
                        "subaward_count": 47,
                        "award_generated_internal_id": "CONT_AWD_HSHQDC17J00370_7001_HSHQDC14DE2035_7001",
                        "subaward_obligation": 25170243.97
                    },
                    {
                        "award_id": "N0042118F0167",
                        "subaward_count": 16,
                        "award_generated_internal_id": "CONT_AWD_N0042118F0167_9700_N0042116D0013_9700",
                        "subaward_obligation": 3587182.87
                    },
                    {
                        "award_id": "N4008518F7138",
                        "subaward_count": 4,
                        "award_generated_internal_id": "CONT_AWD_N4008518F7138_9700_N4008514D7744_9700",
                        "subaward_obligation": 338369.0
                    },
                    {
                        "award_id": "N6247318F4138",
                        "subaward_count": 311,
                        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9700",
                        "subaward_obligation": 134815478.55
                    },
                    {
                        "award_id": "N6247319F4131",
                        "subaward_count": 4,
                        "award_generated_internal_id": "CONT_AWD_N6247319F4131_9700_N6247316D2411_9700",
                        "subaward_obligation": 541913.0
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "hasNext": false
                },
                "messages": [
                    "For searches, time period start and end dates are currently limited to an earliest date of 2007-10-01.  For data going back to 2000-10-01, use either the Custom Award Download feature on the website or one of our download or bulk_download API endpoints as listed on https://api.usaspending.gov/docs/endpoints. "
                ]
            }

# Data Structures

### SubawardGroupedResult (object)
+ `award_id` (required, number)
+ `subaward_count` (required, number)
+ `award_generated_internal_id` (required, string)
+ `subaward_obligation` (required, number)

### PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)

## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` : [`transport`] (optional, array[string])
+ `description` (optional, string)
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: [`Hampton`, `Roads`] (optional, array[string])
    + Text searched across a recipient’s name, UEI, and DUNS
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: [`category_business`, `sole_proprietorship`] (optional, array[string])
+ `award_type_codes` (required, FilterObjectAwardTypes)
+ `award_ids`: [`SPE30018FLGFZ`, `SPE30018FLJFN`] (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: [`10.331`] (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes`: [`J`] (optional, array[string])
+ `set_aside_type_codes`: [`NONE`] (optional, array[string])
+ `extent_competed_type_codes`: [`A`] (optional, array[string])
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)
+ `object_class` (optional, array[string])
+ `program_activity` (optional, array[number])
+ `program_activities` (optional, array[ProgramActivityObject])
    A filter option that supports filtering by a program activity name or code. Please note that if this filter is used at least one of the members of the object, ProgramActivityObject, need to be provided.
+ `def_codes` (optional, array[DEFC], fixed-type)
  If the `def_codes` provided are in the COVID-19 or IIJA group and the subaward flag is set to `False`, the query will only return prime awards that have at least one File C record with the supplied DEFC and also have non-zero COVID-19 or IIJA related obligations or outlays.
  If the `def_codes` provided are in the COVID-19 or IIJA group and the subaward parameter is set to `True`, the query will only return results that have a sub_action_date on or after the enactment date of the public law associated with that disaster code.
    + Example: Providing the `Z` DEF code and setting the subaward parameter to `True` will only return results where the `sub_action_date` is on or after `11/15/2021` since this is the enactment date of the public law associated with disaster code `Z`.
+ `award_unique_id` (optional, string)

### TimePeriodObject (object)
This TimePeriodObject can fall into different categories based on the request.
+ if `subawards` true

    See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

+ otherwise

    See the Award Search category defined in [AwardSearchTimePeriodObject](../../../search_filters.md#award-search-time-period-object)


### LocationObject (object)
These fields are defined in the [StandardLocationObject](../../../search_filters.md#standard-location-object)

### AgencyObject (object)
+ `type` (required, enum[string])
    + Members
        + `awarding`
        + `funding`
+ `tier` (required, enum[string])
    + Members
        + `toptier`
        + `subtier`
+ `name`: `Office of Inspector General` (required, string)
+ `toptier_name`: `Department of the Treasury` (optional, string)
    Only applicable when `tier` is `subtier`.  Ignored when `tier` is `toptier`.  Provides a means by which to scope subtiers with common names to a
    specific toptier.  For example, several agencies have an "Office of Inspector General".  If not provided, subtiers may span more than one toptier.

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### ProgramActivityObject (object)
At least one of the following fields are required when using the ProgramActivityObject.
+ `name`: (optional, string)
+ `code`: (optional, number)

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TreasuryAccountComponentsObject (object)
+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier - three characters
+ `aid` (required, string)
    Agency Identifier - three characters
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability - four digits
+ `epoa` (optional, string, nullable)
    Ending Period of Availability - four digits
+ `a` (optional, string, nullable)
    Availability Type Code - X or null
+ `main` (required, string)
    Main Account Code - four digits
+ `sub` (optional, string, nullable)
    Sub-Account Code - three digits

### FilterObjectAwardTypes (array)
List of filterable award types

#### Sample
- `A`
- `B`
- `C`
- `D`

#### Default
- `02`
- `03`
- `04`
- `05`
- `06`
- `07`
- `08`
- `09`
- `10`
- `11`
- `A`
- `B`
- `C`
- `D`
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)