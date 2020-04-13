FORMAT: 1A
HOST: https://api.usaspending.gov

# TAS

These endpoints are used to power USAspending.gov's TAS search component on the advanced search page.
The response is a forest of filter search nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/tas/{?depth}]

Returns a list of toptier agencies that have at least one TAS affiliated with them
+ Request A request with a contract id
    + Parameters
        + `depth`: (optional, enum[number]) How many levels deep the search will populate each tree. 
            + Members
                    + `0`
                    + `1`
                    + `2`
        0 will return only agencies, 1 will return agencies and any federal accounts under them, and so on.
        + `filter`: (optional, string) When provided, only results who's id or name matches the provided string (case insensitive) will be returned, along with any ancestors to a matching node. 
    + Schema
    
            {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "string"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TASFilterTreeNode], fixed-type)
    + Body

            {
            "results": [
            {
            "id": "012",
            "ancestors": [],
            "description": "Department of Agriculture",
            "count": 139,
            "children": null
            }
            ]
            }
       
## Data Structures

### TASFilterTreeNode (object)

+ `id` (required, string)
+ `description` (required, string)
+ `ancestors` (required, array[string])
+ `count` (required, number)
+ `children` (required, array[TASFilterTreeNode], nullable)