FORMAT: 1A
HOST: https://api.usaspending.gov

# Search for Widgets that meet the criteria [/api/v5/query/widget/]

Nice description of the endpoint. What does it do? What is it for?

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `unique_id` (optional, string)
            The ID to uniquely identifiy widgets and is provided by other endpoints as `widget_id`
        + `size` (optional, enum[string])
            Relative size of the widget
            + Members
                + `small`
                + `medium`
                + `large`
        + `positronic_horsepower` (optional, number)
            Filter widgets to require greater than or equal to the provided computation ability
        + `is_active` (optional, boolean)
            Limit the search to active or deactivated widgets
    + Body

            {
                "unique_id": "4427",
                "is_active": true
            }


+ Response 200 (application/json)
    + Attributes
        + results (required, array[Widget], fixed-type)
        + metadata (required, object)
            + `total` (required, number)
                Total number of widgets matching the search query
            + `count` (required, number)
                Number of widgets returned in the response
            + `next` (required, number, nullable)
                Page number of more results (if exists)
            + `previous` (required, number, nullable)
                Page number of previous results (if exists)

    + Body

            {
                "results": [
                    {
                        "serial": 597,
                        "widget_id": "AB-12323121",
                        "color": "blue",
                        "size": 100732,
                        "model": "Arther-23"
                    },
                    {
                        "serial": 1137,
                        "widget_id": "DD-21287823",
                        "color": "unknown",
                        "size": 140020,
                        "model": "Molli-6"
                    }
                ],
                "metadata": {
                    "total": 2
                    "count": 2
                    "next": null
                    "previous": null
                }
            }

# Data Structures

## Widget (object)
+ `serial` (required, number)
+ `widget_id` (required, string)
+ `color` (required, string, nullable)
+ `size` (required, enum[string])
    + Members
        + small
        + medium
        + large
+ `model` (required, string)