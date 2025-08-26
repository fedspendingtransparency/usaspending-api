FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activities Total [/api/v2/federal_accounts/{FEDERAL_ACCOUNT_CODE}/program_activities/total]

This route returns an array of each program activity's obligation, code, name, and type for the specified federal account 

## GET
+ Parameters
  + `filters`
    + `federal_account` (required string)
    + `time_period` (optional, array[TimePeriod], fixed-type)
    + `object_class` (optional, array[String])
    + `program_activity` (optional, array) 
      Each string should be either "PAC" or "PARK"

+ Response 200 (application/json)
  + Attributes (object)
    + `page_metadata` (required, PageMetadata, fixed-type)
        Information used for pagination of results. 
    + `results` (required, array[ProgramActivitiesTotals], fixed-type)
  + Body
    
        {
            "results":

# Data Structure

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