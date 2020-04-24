FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC

These endpoints are used to power USAspending.gov's PSC search component on the advanced search page.
The response is a forest of filter search nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/psc/{?depth}{?filter}]

Returns the basic groupings of Product Service Codes
+ Request (application/json)
    + Parameters
        + `depth` (optional, enum[number]) 
        How many levels deep the search will populate each tree. 
            + Members
                    + `0`
                    + `1`
                    + `2`
                    + `3`
        + `filter` (optional, string) 
        When provided, only results whose id or name matches the provided string (case insensitive) will be returned, along with any ancestors to a matching node. 
    
    + Schema
    
            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[FilterTreeNode], fixed-type)
    + Body
    
            {
            "results": [
            {
            "id": "Research and Development",
            "ancestors": [],
            "description": "",
            "count": 21,
            "children": null
            },
            {
            "id": "Service",
            "ancestors": [],
            "description": "",
            "count": 23,
            "children": null
            },
            {
            "id": "Product",
            "ancestors": [],
            "description": "",
            "count": 77,
            "children": null
            }
            ]
            }
       
## Data Structures

### FilterTreeNode (object)

+ `id` (required, string)
+ `description` (required, string)
+ `ancestors` (required, array[string])
+ `count` (required, number)
+ `children` (required, array[FilterTreeNode], nullable)