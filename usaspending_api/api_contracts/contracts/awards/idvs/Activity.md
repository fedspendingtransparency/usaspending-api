FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Activity [/api/v2/awards/idvs/activity/]

This endpoint is used to power the IDV (Indefinite Delivery Vehicle) Activity visualization on IDV Summary Pages. It returns information about child awards and grandchild awards for a given IDV.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AW_4730_-NONE-_GS23F0170L_-NONE-` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `limit`: 10 (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page`: 1 (optional, number)
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
                        "generated_unique_award_id": "CONT_AW_1425_4730_INR17PA00008_GS23F0170L",
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
                        "generated_unique_award_id": "CONT_AW_4732_4730_GS33FCA001_GS23F0170L",
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
                        "generated_unique_award_id": "CONT_AW_12D2_4730_AG3151B140009_GS23F0170L",
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
                    "count": 40,
                    "page": 1
                }
            }

# Data Structures

## PageMetaData (object)
+ `page`: 2 (required, number)
+ `count`: 40 (required, number)
    The total number of results.

## ChildAward (object)
+ `award_id`: 69054107 (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `generated_unique_award_id`: `CONT_AW_1540_NONE_DJB30605051_NONE` (required, string)
    Unique internal natural identifier for an award.
+ `awarding_agency`: `GENERAL SERVICES ADMINISTRATION (GSA)` (required, string)
+ `awarding_agency_id`: 634 (required, number)
+ `last_date_to_order`: `2017-09-30` (required, string, nullable)
+ `obligated_amount`: 2257.24 (required, number)
+ `awarded_amount`: 10000.00 (required, number)
+ `period_of_performance_start_date`: `2014-10-01` (required, string, nullable)
    The starting date of the award in the format `YYYY-MM-DD`
+ `piid`: `GS33FCA001` (required, string)
    Procurement Instrument Identifier (PIID).
+ `recipient_name`: `Booz Allen Hamilton` (required, string, nullable)
+ `recipient_id`: `9a277fc5-50fc-685f-0f77-be0d96420a17-C` (required, string, nullable)
+ `grandchild`: false (required, boolean)
