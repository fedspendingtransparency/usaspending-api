FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC

This endpoint is used to power USAspending.gov's "Product or Service Code" search component on the advanced search page.
The response is a forest of search filter nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/psc/{group}/{psc}/{psc2}/{?depth}{?filter}]

Returns PSCs under the provided PSC
+ Request (application/json)
    + Parameters
        + `group`: `Product` (required, string)
        Parent group of PSCs to return. Currently all PSCs are grouped under either "Product", "Service", or "Research and Development".
        + `psc`: `10` (required, string)
        ID value of the grandparent node
        + `psc2`: `1000` (required, string)
        ID value of the parent node
        + `depth` (optional, number) 
            Defines how many levels of descendants to return under each node. For example, depth=0 will 
            return a flat array, while depth=2 will populate the children array of each top level node 
            with that node's children and grandchildren. The actual depth of each tree may be less than 
            the value of depth if returned nodes have no children. Negative values are treated as 
            infinite, returning all descendants.
            + Default: 0
        
        + `filter` (optional, string)                 
            Restricts results to nodes with a `id` or `description` matching the filter string. If depth is 
            greater than zero, nodes will also appear the response if at least one child within depth 
            matches the filter.
    
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
+ `children` (required, array[FilterTreeNode], nullable, fixed-type)