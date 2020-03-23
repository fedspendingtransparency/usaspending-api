FORMAT: 1A
HOST: https://api.usaspending.gov

# TAS

These endpoints are used to power USAspending.gov's TAS search component on the advanced search page.
The response is a forest of filter search nodes, which despite having a unified structure represent different
database fields based on depth in the tree.

## Toptier Search [GET /api/v2/references/filter_tree/tas/{?depth}]

Returns a list of toptier agencies that have at least one TAS affiliated with them
+ Request A request with a contract id (application/json)
    + Parameters
        + `depth`: `0` (optional, enum[number]) How many levels deep the search will populate each tree. 
            + Members
                    + `0`
                    + `1`
                    + `2`
        0 will return only agencies, 1 will return agencies and any federal accounts under them, and so on.
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TASFilterTreeNode], fixed-type)

## Search by Agency [GET /api/v2/references/filter_tree/tas/{agency}/{?depth}]

Returns a list of federal accounts associated with the specified agency
+ Request A request with a naics id (application/json)
    + Parameters
        + `agency`: `020` (required, string) 
        + `depth`: `0` (optional, enum[number]) How many levels deep the search will populate each tree. 
            + Members
                    + `0`
                    + `1`
                    + `2`        
        0 will return only federal accounts, and 1 will return federal accounts and any TAS under them

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TASFilterTreeNode], fixed-type)

## Search by Federal Account [GET /api/v2/references/filter_tree/tas/{agency}/{federal_account}/{?depth}]

Returns a list of Treasury Account Symbols associated with the specified federal account
+ Request A request with a naics id (application/json)
    + Parameters
        + `agency`: `020` (required, string) 
        + `federal_account`: `0550`
        + `depth`: `0` (optional, enum[number]) How many levels deep the search will populate each tree.
            + Members
                    + `0`
                    + `1`
                    + `2` 
        With this tree structure, only TAS will be returned, and the tree depth will always be one, reguardless of provided depth.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TASFilterTreeNode], fixed-type)

## Data Structures

### TASFilterTreeNode (object)

+ `id` (required, string)
+ `description` (required, string)
+ `ancestors` (required, array[string])
+ `count` (required, number)
+ `children` (required, array[TASFilterTreeNode], nullable)