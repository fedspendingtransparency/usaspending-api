FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Related Awards [/api/v2/idvs/awards/]

## POST

List child IDVs, child awards, or grandchild awards for IDV

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_GS23F0170L_4730` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `type`: `child_idvs` (optional, enum[string])
            The type of related awards to return.
            + Default: `child_idvs`
            + Members
                + `child_awards`
                + `grandchild_awards`
        + `limit`: 5 (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page`: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `sort`: `period_of_performance_start_date` (optional, enum[string])
            The field results are sorted by.
            + Default: `period_of_performance_start_date`
            + Members
                + `piid`
                + `description`
                + `period_of_performance_current_end_date`
                + `last_date_to_order`
                + `funding_agency`
                + `awarding_agency`
                + `award_type`
                + `obligated_amount`
        + `order`: `desc` (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: `desc`            
    + Body
            
            
            {
                "award_id": "CONT_IDV_TMHQ10C0040_2044",
                "type": "child_awards",
                "limit": 10,
                "page": 1,
                "sort": "period_of_performance_start_date",
                "order": "desc"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[IDVRelatedAwardsResponse], fixed-type)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)

    * Body

           
            {
                "results": [
                    {
                        "award_id": 69138778,
                        "award_type": "BPA",
                        "description": "IGF::OT::IGF GP-2600 FEDEX SHIPPING SERVICE - GREAT PLAINS REGIONAL OFFICE, BILLINGS, MONTANA",
                        "funding_agency": "DEPARTMENT OF THE INTERIOR",
                        "awarding_agency": "DEPARTMENT OF THE INTERIOR",
                        "funding_agency_id": 228,
                        "awarding_agency_id": 228,
                        "funding_agency_slug": "department-of-the-interior",
                        "awarding_agency_slug": "department-of-the-interior",
                        "generated_unique_award_id": "CONT_IDV_INR17PA00008_1425",
                        "last_date_to_order": "2020-11-30",
                        "obligated_amount": 8000.0,
                        "period_of_performance_current_end_date": null,
                        "period_of_performance_start_date": "2016-01-14",
                        "piid": "INR17PA00008"
                    },
                    {
                        "award_id": 69054107,
                        "award_type": "BPA",
                        "description": "OTHER THAN SCHEDULE,IGF::OT::IGF",
                        "funding_agency": "GENERAL SERVICES ADMINISTRATION",
                        "awarding_agency": "GENERAL SERVICES ADMINISTRATION",
                        "funding_agency_id": 634,
                        "awarding_agency_id:: 634,
                        "funding_agency_slug": "general-services-administration",
                        "awarding_agency_slug": "general-services-administration",
                        "generated_unique_award_id": "CONT_IDV_GS33FCA001_4732",
                        "last_date_to_order": "2017-09-30",
                        "obligated_amount": 22570355.24,
                        "period_of_performance_current_end_date": null,
                        "period_of_performance_start_date": "2014-10-01",
                        "piid": "GS33FCA001"
                    },
                    {
                        "award_id": 69216438,
                        "award_type": "BPA",
                        "description": "IGF::OT::IGF BPA WITH AN INTERNATIONAL MAIL DELIVERY SERVICE FOR PACKAGES GENERATED THROUGH TSED'S COCHRAN FELLOWSHIP PROGRAM",
                        "funding_agency": "DEPARTMENT OF AGRICULTURE",
                        "awarding_agency": "DEPARTMENT OF AGRICULTURE",
                        "funding_agency_id": 153,
                        "awarding_agency_id": 153,
                        "funding_agency_slug": "department-of-agriculture",
                        "awarding_agency_slug": "department-of-agriculture",
                        "generated_unique_award_id": "CONT_IDV_AG3151B140009_12D2",
                        "last_date_to_order": "2015-04-06",
                        "obligated_amount": 47840.0,
                        "period_of_performance_current_end_date": null,
                        "period_of_performance_start_date": "2014-04-07",
                        "piid": "AG3151B140009"
                    }
                ],
                "page_metadata": {
                    "hasNext": true,
                    "hasPrevious": false,
                    "next": 2,
                    "page": 1,
                    "previous": null
                }
            }

# Data Structures

## PageMetaDataObject (object)
+ `page`: 2 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next`: 3 (required, number, nullable)
+ `previous`: 1 (required, number, nullable)

## IDVRelatedAwardsResponse (object)
+ `award_id`: 69054107 (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `award_type`: `BPA`(required, string)
+ `description`: `OTHER THAN SCHEDULE,IGF::OT::IGF` (required, string, nullable)
+ `funding_agency`: `GENERAL SERVICES ADMINISTRATION (GSA)` (required, string, nullable)
+ `awarding_agency`: `GENERAL SERVICES ADMINISTRATION (GSA)` (required, string, nullable)
+ `funding_agency_id`: 634 (required, number, nullable)
+ `awarding_agency_id`: 634 (required, number, nullable)
+ `funding_agency_slug`: `general-services-administration-gsa` (required, string, nullable)
    `funding_agency_slug` is a string used to generate a link to the funding agency's profile page. Will be `NULL` if the funding agency does not have a profile page.
+ `awarding_agency_slug`: `general-services-administration-gsa` (required, string, nullable)
    `awarding_agency_slug` is a string used to generate a link to the awarding agency's profile page. Will be `NULL` if the awarding agency does not have a profile page.
+ `generated_unique_award_id`: `CONT_IDV_DJB30605051_1540` (required, string)
    Unique internal natural identifier for an award.
+ `last_date_to_order`: `2017-09-30` (required, string, nullable)
+ `obligated_amount`: 22570355.24 (required, number)
+ `period_of_performance_current_end_date`: `2015-02-19` (required, string, nullable)
    The ending date of the award in the format `YYYY-MM-DD`
+ `period_of_performance_start_date`: `2014-10-01` (required, string, nullable)
    The starting date of the award in the format `YYYY-MM-DD`
+ `piid`: `GS33FCA001` (required, string)
    Procurement Instrument Identifier (PIID).
