FORMAT: 1A
HOST: https://api.usaspending.gov

# TAS

These endpoints are used to power USAspending.gov's TAS search component on the advanced search page.
The response is a forest of filter search nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Search by Federal Account [GET /api/v2/references/filter_tree/tas/{agency}/{federal_account}/{?depth}]

Returns a list of Treasury Account Symbols associated with the specified federal account
+ Request (application/json)
    + Parameters
        + `agency`: `020` (required, string) 
        + `federal_account`: `0550`
        + `depth` (optional, enum[number]) 
        How many levels deep the search will populate each tree.
            + Members
                    + `0`
                    + `1`
                    + `2` 
        With this tree structure, only TAS will be returned, and the tree depth will always be one, regardless of provided depth.
        + `filter` (optional, string) 
        When provided, only results who's id or name matches the provided string (case insensitive) will be returned, along with any ancestors to a matching node.
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
            "id": "075-2016/2017-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2017/2017-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2013/2013-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2019/2019-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2020/2020-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2018/2018-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2016/2016-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2015/2015-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-X-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health, Health and Human Services",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2014/2014-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health",
            "count": 0,
            "children": null
            },
            {
            "id": "075-2012/2012-0884-000",
            "ancestors": [
            "073",
            "075-0884"
            ],
            "description": "National Institute of Diabetes and Digestive and Kidney Diseases, National Institutes of Health",
            "count": 0,
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