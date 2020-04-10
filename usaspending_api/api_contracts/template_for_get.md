FORMAT: 1A
HOST: https://api.usaspending.gov

# Short Endpoint Name [/api/v2/this/is/your/{param_for_endpoint}/]

Description of the endpoint as a whole not taking into account the different HTTP methods.

## GET

Description of the endpoint using the above HTTP method.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `param_for_endpoint`: `endpoint` (required, string)
            Description of the parameter if not obvious.


+ Response 200 (application/json)
    + Attributes
        + `data_structure_array` (required, array[ForTheArray], fixed-type)
        + `single_data_structure` (optional, SampleSingleObject)
        + `name` (required, enum[string])
            + Members
                + `A`
                + `B`
                + `C`

    + Body

            {
                "data_structure_array": [
                    [25, 43],
                    [19, -67]
                ],
                "name": "A"
            }

# Data Structures

## ForTheArray
+ `value_1` (optional, string)
+ `value_2` (optional, string)

## SampleSingleObject
+ `value_1` (required, number)
+ `value_2` (optional, string)
+ `value_3` (optional, number, nullable)
