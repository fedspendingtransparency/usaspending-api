FORMAT: 1A
HOST: https://api.usaspending.gov

# Short Endpoint Name [/api/v2/this/is/your/{param_for_endpoint}/]

Description of the endpoint as a whole not taking into account the different HTTP methods.

## GET

Description of the endpoint using the above HTTP method.

+ Parameters
    + `param_for_endpoint`: `endpoint` (required, string)
        Description of the parameter if not obvious.
        
+ Request (application/json)
    + Attributes
        + `string_attribute`: `hello world` (required, string)
        + `optional_attribute` (optional, string, nullable)
        + `number_attribute`: 123456.78 (required, number)
        + `boolean_attribute`: true (optional, boolean)

+ Response 200 (application/json)
    + Attributes
        + `data_structure_array` (required, array[ForTheArray], fixed-type)
        + `single_data_structure` (optional, SampleSingleObject)
        + `name` (required, enum[string])
            + Members
                + `A`
                + `B`
                + `C`

# Data Structures

## ForTheArray
+ `value_1` (optional, string)
+ `value_2` (optional, string)

## SampleSingleObject
+ `value_1` (required, number)
+ `value_2` (optional, string)
+ `value_3` (optional, number, nullable)
