FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activities Total [/api/v2/federal_accounts/{FEDERAL_ACCOUNT_CODE}/program_activities/total]

This endpoint returns an array of each program activity's obligation, code, name, and type for the specified federal account 

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (optional, ProgramActivityTotalsFilterObject)
            This can filter by time period, object class, and/or program activity being `PARK` or `PAC/PAN`
        + `limit` (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page` (optional, number)
            The page of results to return based on the limit.
            + Default: 1
  
    + Body
        
            {
                "filters": {
                    "time_period": "[
                        {
                            "start_date": "2019-10-01",
                            "end_date": "2020-09-30"
                        }]",
                    "program_activity": ['PARK']
                },
                "sort": {
                    "direction": "asc",
                    "field": "account_name"
                }
            }


+ Response 200 (application/json)
  + Attributes (object)
    + `page_metadata` (required, PageMetadata, fixed-type)
        Information used for pagination of results. 
    + `results` (required, array[ProgramActivitiesTotals], fixed-type)
  + Body
    
            {
                "results": [
                  {"obligations": 44112.0, "code": "00000000003", "name": "PARK 3", "type": "PARK"},
                  {"obligations": 6000.0, "code": "00000000001", "name": "PARK 1", "type": "PARK"},
                  {"obligations": 130.0, "code": "00000000002", "name": "PARK 2", "type": "PARK"},
                  {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
                ],
                "page_metadata": {
                  "page": 1,
                  "total": 4,
                  "limit": 10,
                  "next": None,
                  "previous": None,
                  "hasNext": False,
                  "hasPrevious": False,
                },
            }


# Data Structure

## ProgramActivityTotalsFilterObject (object)
+ `federal_account_code`: `431-0500` (required, string)
Federal account code consisting of the AID and main account code
+ `time_period`: `[
                        {
                            "start_date": "2019-10-01",
                            "end_date": "2020-09-30"
                        }]`(optional, array[TimePeriod], fixed-type)
+ `object_class`: `["254"]` (optional, array[String])
+ `program_activity`: `["PARK"]` (optional, array[String])
  Each string should be either "PAC" or "PARK"

## ProgramActivitiesTotals (object)
+ `obligations` (required, number)
+ `code` (required, string)
+ `name` (required, string)
+ `type` (required, enum[string], fixed-type)
  Whether the Program Activity values are from the older Program Activity Code / Name (PAC/PAN) or the Program Activity Reporting Key (PARK)
  + Members
    + `PAC/PAN`
    + `PARK`

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)