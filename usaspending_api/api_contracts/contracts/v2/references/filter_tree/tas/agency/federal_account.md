FORMAT: 1A
HOST: https://api.usaspending.gov

# TAS

This endpoint is used to power USAspending.gov's TAS search component on the advanced search page.
The response is a forest of search filter nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Search by Federal Account [GET /api/v2/references/filter_tree/tas/{agency}/{federal_account}/{?depth}]

Returns a list of Treasury Account Symbols associated with the specified federal account
+ Request (application/json)
    + Parameters
        + `agency`: `020` (required, string)
        + `federal_account`: `0550`
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

### FilterTreeNode (object)

+ `id` (required, string)
+ `description` (required, string)
+ `ancestors` (required, array[string])
+ `count` (required, number)
+ `children` (required, array[FilterTreeNode], nullable, fixed-type)
