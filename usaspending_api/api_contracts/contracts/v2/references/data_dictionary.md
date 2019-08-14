FORMAT: 1A
HOST: https://api.usaspending.gov

# Data Dictionary [/api/v2/references/data_dictionary/]

This endpoint powers USAspending.gov's data dictionary page.

## GET

This endpoint returns data corresponding to the latest data dictionary csv file.

+ Response 200 (application/json)
    + Attributes
        + `document` (DataDictionary)

# Data Structures

## DataDictionary (object)
+ `metadata` (required, DictionaryMetadata)
+ `sections` (array[Section], fixed-type)
+ `headers` (array[Column], fixed-type)
+ `rows` (required, array, fixed-type)
    + `Lorem ipsum`, `dolor sit amet`, `consectetur adipiscing elit` (array[string], nullable)

## Section (object)
+ `section`: `Data Labels` (required, string)
+ `colspan`: 2 (required, number)
    The number of columns in the section

## Column (object)
+ `raw`: `award_file` (required, string)
+ `display`: `Award File` (required, string)

## DictionaryMetadata (object)
+ `total_rows`: 393 (required, number)
+ `total_columns`: 12 (required, number)
+ `total_size`: `119.32KB` (required, string)
+ `download_location`: `https://files.usaspending.gov/docs/DATA+Transparency+Crosswalk.xlsx` (required, string)
