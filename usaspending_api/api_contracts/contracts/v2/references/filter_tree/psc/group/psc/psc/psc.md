FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC

These endpoints are used to power USAspending.gov's Product Service Code search component on the advanced search page.
The response is a forest of filter search nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/psc/{group}/{psc}/{psc2}/{?depth}{?filter}]

Returns PSCs under the provided PSC
+ Request (application/json)
    + Parameters
        + `group`: `Product` (required, string)
        Parent group of PSCs to return
        + `psc`: `10` (required, string)
        ID value of the grandparent node
        + `psc2`: `1000` (required, string)
        ID value of the parent node
        + `depth` (optional, enum[number]) 
            + Default: 0
            + Members
                    + 0
                    + 1
                    + 2
                    + 3
        Depth to populate for each tree returned in results. Trees will only populate with elements that match the filter provided, if any.
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
                    "id": "AA",
                    "ancestors": [
                        "Research and Development"
                    ],
                    "description": "AGRICULTURE R&D",
                    "count": 4,
                    "children": null
                    },
                    {
                    "id": "F",
                    "ancestors": [
                        "Service"
                    ],
                    "description": "NATURAL RESOURCES MANAGEMENT",
                    "count": 3,
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