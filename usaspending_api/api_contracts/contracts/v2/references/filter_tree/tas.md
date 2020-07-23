FORMAT: 1A
HOST: https://api.usaspending.gov

# TAS

This endpoint is used to power USAspending.gov's TAS search component on the advanced search page.
The response is a forest of search filter nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/tas/{?depth}]

Returns a list of toptier agencies that have at least one TAS affiliated with them
+ Request (application/json)
    + Parameters
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
                        "id": "012",
                        "ancestors": [],
                        "description": "Department of Agriculture (USDA)",
                        "count": 569,
                        "children": null
                    },
                    {
                        "id": "013",
                        "ancestors": [],
                        "description": "Department of Commerce (DOC)",
                        "count": 165,
                        "children": null
                    },
                    {
                        "id": "097",
                        "ancestors": [],
                        "description": "Department of Defense (DOD)",
                        "count": 541,
                        "children": null
                    },
                    {
                        "id": "091",
                        "ancestors": [],
                        "description": "Department of Education (ED)",
                        "count": 254,
                        "children": null
                    },
                    {
                        "id": "089",
                        "ancestors": [],
                        "description": "Department of Energy (DOE)",
                        "count": 177,
                        "children": null
                    },
                    {
                        "id": "075",
                        "ancestors": [],
                        "description": "Department of Health and Human Services (HHS)",
                        "count": 1056,
                        "children": null
                    },
                    {
                        "id": "070",
                        "ancestors": [],
                        "description": "Department of Homeland Security (DHS)",
                        "count": 586,
                        "children": null
                    },
                    {
                        "id": "086",
                        "ancestors": [],
                        "description": "Department of Housing and Urban Development (HUD)",
                        "count": 319,
                        "children": null
                    },
                    {
                        "id": "015",
                        "ancestors": [],
                        "description": "Department of Justice (DOJ)",
                        "count": 174,
                        "children": null
                    },
                    {
                        "id": "1601",
                        "ancestors": [],
                        "description": "Department of Labor (DOL)",
                        "count": 273,
                        "children": null
                    },
                    {
                        "id": "019",
                        "ancestors": [],
                        "description": "Department of State (DOS)",
                        "count": 291,
                        "children": null
                    },
                    {
                        "id": "014",
                        "ancestors": [],
                        "description": "Department of the Interior (DOI)",
                        "count": 356,
                        "children": null
                    },
                    {
                        "id": "020",
                        "ancestors": [],
                        "description": "Department of the Treasury (TREAS)",
                        "count": 283,
                        "children": null
                    },
                    {
                        "id": "069",
                        "ancestors": [],
                        "description": "Department of Transportation (DOT)",
                        "count": 383,
                        "children": null
                    },
                    {
                        "id": "036",
                        "ancestors": [],
                        "description": "Department of Veterans Affairs (VA)",
                        "count": 2,
                        "children": null
                    },
                    {
                        "id": "068",
                        "ancestors": [],
                        "description": "Environmental Protection Agency (EPA)",
                        "count": 55,
                        "children": null
                    },
                    {
                        "id": "047",
                        "ancestors": [],
                        "description": "General Services Administration (GSA)",
                        "count": 53,
                        "children": null
                    },
                    {
                        "id": "080",
                        "ancestors": [],
                        "description": "National Aeronautics and Space Administration (NASA)",
                        "count": 130,
                        "children": null
                    },
                    {
                        "id": "049",
                        "ancestors": [],
                        "description": "National Science Foundation (NSF)",
                        "count": 54,
                        "children": null
                    },
                    {
                        "id": "031",
                        "ancestors": [],
                        "description": "Nuclear Regulatory Commission (NRC)",
                        "count": 3,
                        "children": null
                    },
                    {
                        "id": "024",
                        "ancestors": [],
                        "description": "Office of Personnel Management (OPM)",
                        "count": 20,
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
