FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Activity [/api/v2/awards/idvs/activity/]

This endpoint is used to power the IDV (Indefinite Delivery Vehicle) Activity visualization on IDV Summary Pages. It returns information about child awards and grandchild awards for a given IDV.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_GS23F0170L_4730` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `limit` (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page` (optional, number)
            The page of results to return based on the limit.
            + Default: 1

+ Response 200 (application/json)
    + Attributes
        + `results` (required, array[ChildAward], fixed-type)
            Results are sorted by awarded amount in descending order.
        + `page_metadata` (required, PageMetaData)

    * Body

            {
                "results": [
                    {
                        "award_id": 69138778,
                        "awarding_agency": "DEPARTMENT OF THE INTERIOR (DOI)",
                        "awarding_agency_id": 228,
                        "generated_unique_award_id": "CONT_IDV_INR17PA00008_1425",
                        "last_date_to_order": "2020-11-30",
                        "obligated_amount": 8000.0,
                        "awarded_amount": 20000.0,
                        "period_of_performance_start_date": "2016-01-14",
                        "piid": "INR17PA00008",
                        "recipient_name": "Booz Allen Hamilton",
                        "recipient_id": "543ee6af-9096-f32a-abaa-834106bead6a-P",
                        "grandchild": false
                    },
                    {
                        "award_id": 69054107,
                        "awarding_agency": "GENERAL SERVICES ADMINISTRATION (GSA)",
                        "awarding_agency_id": 634,
                        "generated_unique_award_id": "CONT_IDV_GS33FCA001_4732",
                        "last_date_to_order": "2017-09-30",
                        "obligated_amount": 2257.24,
                        "awarded_amount": 10000.0,
                        "period_of_performance_start_date": "2014-10-01",
                        "piid": "GS33FCA001",
                        "recipient_name": "Booz Allen Hamilton",
                        "recipient_id": "9a277fc5-50fc-685f-0f77-be0d96420a17-C",
                        "grandchild": false
                    },
                    {
                        "award_id": 69216438,
                        "awarding_agency": "DEPARTMENT OF AGRICULTURE (USDA)",
                        "awarding_agency_id": 153,
                        "generated_unique_award_id": "CONT_IDV_AG3151B140009_12D2",
                        "last_date_to_order": "2015-04-06",
                        "obligated_amount": 47840.0,
                        "awarded_amount": 12000.0,
                        "period_of_performance_start_date": "2014-04-07",
                        "piid": "AG3151B140009",
                        "recipient_name": "Booz Allen Hamilton",
                        "recipient_id": "9a277fc5-50fc-685f-0f77-be0d96420a17-C",
                        "grandchild": true
                    }
                ],
                "page_metadata": {
                    "hasNext": true,
                    "hasPrevious": true,
                    "limit": 3,
                    "next": 5,
                    "page": 4,
                    "previous": 3,
                    "total": 30066
                }
            }

# Data Structures

## PageMetaData (object)
+ `hasNext` (boolean, required)
+ `hasPrevious` (boolean, required)
+ `limit` (required, number)
+ `next` (number, required, nullable)
+ `page` (number, required)
+ `previous` (number, required, nullable)
+ `total` (required, number)
    Total count of all results including those not returned on this page.

## ChildAward (object)
+ `award_id` (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `generated_unique_award_id` (required, string)
    Unique internal natural identifier for an award.
+ `awarding_agency` (required, string)
+ `awarding_agency_id` (required, number)
+ `last_date_to_order` (required, string, nullable)
+ `obligated_amount` (required, number)
+ `awarded_amount` (required, number)
+ `period_of_performance_start_date` (required, string, nullable)
    The starting date of the award in the format `YYYY-MM-DD`
+ `piid` (required, string)
    Procurement Instrument Identifier (PIID).
+ `recipient_name` (required, string, nullable)
+ `recipient_id` (required, string, nullable)
+ `grandchild` (required, boolean)
