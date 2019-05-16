FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Activity [/api/v2/awards/idvs/activity/]

This endpoint is used to power the IDV (Indefinite Delivery Vehicle) Activity visualization on IDV Summary Pages. It returns information about child awards and grandchild awards for a given IDV.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AW_4730_-NONE-_GS23F0170L_-NONE-` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + limit: 10 (optional, number)
            The number of results to include per page.
            + Default: 10
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1

+ Response 200 (application/json)
    + Attributes
        + results (array[ChildAward], fixed-type)
            Results are sorted by obligated amount in descending order.
        + page_metadata (PageMetaData)

# Data Structures

## PageMetaData (object)
+ page: 2 (required, number)
+ count: 40 (required, number)
    The total number of results.

## ChildAward (object)
+ `award_id`: `69054107` (required, string)
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
