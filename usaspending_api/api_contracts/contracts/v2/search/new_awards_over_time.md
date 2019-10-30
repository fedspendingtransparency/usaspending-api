FORMAT: 1A
HOST: https://api.usaspending.gov


# New Awards Over Time [/api/v2/search/new_awards_over_time/]

This endpoint is used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## POST

This endpoint returns the count of new awards grouped by time period in ascending order (earliest to most recent).

+ Request (application/json)
    + Attributes (object)
        + group: `quarter` (required, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `month`
        + `filters` (required, TimeFilterObject)

+ Response 200 (application/json)
    + Attributes (object)
        + `group`: `quarter` (required, enum[string])
           + Members
                + `fiscal_year`
                + `quarter`
                + `month`
        + `results` (array[TimeResult], fixed-type)

# Data Structures

## TimeResult (object)
+ `time_period` (required, TimePeriodGroup)
+ `new_award_count_in_period`: 25 (required, number)
    The count of new awards for this time period and the given filters.

## TimeFilterObject (object)
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `recipient_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-P` (optional, string)
    A hash of recipient DUNS, name, and level. A unique identifier for recipients.

## TimePeriodGroup (object)
+ `fiscal_year`: `2018` (required, string)
+ `quarter`: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `month`.
+ `month`: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `quarter`.

## TimePeriodObject (object)
+ `start_date`: `2016-10-01` (required, string)
+ `end_date`: `2017-09-30` (required, string)
