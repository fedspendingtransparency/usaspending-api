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
        + Body
```
            {
  "results": [
    {
      "id": "012",
      "ancestors": [],
      "description": "Department of Agriculture",
      "count": 139,
      "children": [
        {
          "id": "012-8226",
          "ancestors": [
            "012"
          ],
          "description": "Miscellaneous Contributed Funds, Animal and Plant Health Inspection Service, Agriculture",
          "count": 1,
          "children": [
            {
              "id": "012-X-8226-000",
              "ancestors": [
                "012",
                "012-8226"
              ],
              "description": "Miscellaneous Contributed Funds, Animal and Plant Health Inspection Service, Agriculture",
              "count": 0,
              "children": null
            }
          ]
        },
        {
          "id": "012-8214",
          "ancestors": [
            "012"
          ],
          "description": "Miscellaneous Contributed Funds, Agricultural Research Service, Agriculture",
          "count": 1,
          "children": [
            {
              "id": "012-X-8214-000",
              "ancestors": [
                "012",
                "012-8214"
              ],
              "description": "Miscellaneous Contributed Funds, Agricultural Research Service, Agriculture",
              "count": 0,
              "children": null
            }
          ]
        }
      ]
    }
  ]
}
```

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
    + Body
```
{
  "results": [
    {
      "id": "012-8226",
      "ancestors": [
        "012"
      ],
      "description": "Miscellaneous Contributed Funds, Animal and Plant Health Inspection Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8226-000",
          "ancestors": [
            "012",
            "012-8226"
          ],
          "description": "Miscellaneous Contributed Funds, Animal and Plant Health Inspection Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8214",
      "ancestors": [
        "012"
      ],
      "description": "Miscellaneous Contributed Funds, Agricultural Research Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8214-000",
          "ancestors": [
            "012",
            "012-8214"
          ],
          "description": "Miscellaneous Contributed Funds, Agricultural Research Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8203",
      "ancestors": [
        "012"
      ],
      "description": "Gifts and Bequests, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8203-000",
          "ancestors": [
            "012",
            "012-8203"
          ],
          "description": "Gifts and Bequests, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8161",
      "ancestors": [
        "012"
      ],
      "description": "Tobacco Trust Fund, Commodity Credit Corporation, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8161-000",
          "ancestors": [
            "012",
            "012-8161"
          ],
          "description": "Tobacco Trust Fund, Commodity Credit Corporation, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8100",
      "ancestors": [
        "012"
      ],
      "description": "Wool Research, Development and Promotion Trust Fund, Agricultural Marketing Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8100-000",
          "ancestors": [
            "012",
            "012-8100"
          ],
          "description": "Wool Research, Development and Promotion Trust Fund, Agricultural Marketing Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8046",
      "ancestors": [
        "012"
      ],
      "description": "Reforestation Trust Fund, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8046-000",
          "ancestors": [
            "012",
            "012-8046"
          ],
          "description": "Reforestation Trust Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8039",
      "ancestors": [
        "012"
      ],
      "description": "Land Between the Lakes Trust Fund, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8039-000",
          "ancestors": [
            "012",
            "012-8039"
          ],
          "description": "Land Between the Lakes Trust Fund, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8028",
      "ancestors": [
        "012"
      ],
      "description": "Cooperative Work, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8028-000",
          "ancestors": [
            "012",
            "012-8028"
          ],
          "description": "Cooperative Work, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-8015",
      "ancestors": [
        "012"
      ],
      "description": "Expenses and Refunds, Inspection and Grading of Farm Products, Agricultural Marketing Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-8015-000",
          "ancestors": [
            "012",
            "012-8015"
          ],
          "description": "Expenses and Refunds, Inspection and Grading of Farm Products, Agricultural Marketing Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5896",
      "ancestors": [
        "012"
      ],
      "description": "Payments to Counties, National Grasslands, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5896-000",
          "ancestors": [
            "012",
            "012-5896"
          ],
          "description": "Payments to Counties, National Grasslands, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5540",
      "ancestors": [
        "012"
      ],
      "description": "Stewardship Contracting Product Sales, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5540-000",
          "ancestors": [
            "012",
            "012-5540"
          ],
          "description": "Stewardship Contracting Product Sales, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5531",
      "ancestors": [
        "012"
      ],
      "description": "Agricultural Disaster Relief Trust Fund, Farm Service Agency, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5531-000",
          "ancestors": [
            "012",
            "012-5531"
          ],
          "description": "Agricultural Disaster Relief Trust Fund, Farm Service Agency, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5462",
      "ancestors": [
        "012"
      ],
      "description": "Hardwood Technology Transfer and Applied Research Fund, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5462-000",
          "ancestors": [
            "012",
            "012-5462"
          ],
          "description": "Hardwood Technology Transfer and Applied Research Fund, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5411",
      "ancestors": [
        "012"
      ],
      "description": "Inspector General Assets Forfeiture, Treasury",
      "count": 1,
      "children": [
        {
          "id": "012-X-5411-000",
          "ancestors": [
            "012",
            "012-5411"
          ],
          "description": "Inspector General Assets Forfeiture, Treasury",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5410",
      "ancestors": [
        "012"
      ],
      "description": "Inspector General Assets Forfeiture, Justice",
      "count": 1,
      "children": [
        {
          "id": "012-X-5410-000",
          "ancestors": [
            "012",
            "012-5410"
          ],
          "description": "Inspector General Assets Forfeiture, Justice",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5367",
      "ancestors": [
        "012"
      ],
      "description": "State and Private Forestry, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5367-000",
          "ancestors": [
            "012",
            "012-5367"
          ],
          "description": "State and Private Forestry, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5361",
      "ancestors": [
        "012"
      ],
      "description": "Administration of Rights-of-Way and Other Land Uses Fund, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5361-000",
          "ancestors": [
            "012",
            "012-5361"
          ],
          "description": "Administration of Rights-of-Way and Other Land Uses Fund, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5360",
      "ancestors": [
        "012"
      ],
      "description": "Land Between the Lakes Management Fund, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5360-000",
          "ancestors": [
            "012",
            "012-5360"
          ],
          "description": "Land Between the Lakes Management Fund, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5279",
      "ancestors": [
        "012"
      ],
      "description": "Concessions Fees and Volunteer Services, Agricultural Research Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5279-000",
          "ancestors": [
            "012",
            "012-5279"
          ],
          "description": "Concessions Fees and Volunteer Services, Agricultural Research Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5277",
      "ancestors": [
        "012"
      ],
      "description": "MNP Rental Fee Account, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5277-000",
          "ancestors": [
            "012",
            "012-5277"
          ],
          "description": "MNP Rental Fee Account, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5268",
      "ancestors": [
        "012"
      ],
      "description": "Recreation Fee Demonstration Program, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5268-000",
          "ancestors": [
            "012",
            "012-5268"
          ],
          "description": "Recreation Fee Demonstration Program, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5264",
      "ancestors": [
        "012"
      ],
      "description": "Timber Sales Pipeline Restoration Fund, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5264-000",
          "ancestors": [
            "012",
            "012-5264"
          ],
          "description": "Timber Sales Pipeline Restoration Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5219",
      "ancestors": [
        "012"
      ],
      "description": "Operation and Maintenance of Quarters, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5219-000",
          "ancestors": [
            "012",
            "012-5219"
          ],
          "description": "Operation and Maintenance of Quarters, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5216",
      "ancestors": [
        "012"
      ],
      "description": "Acquisition of Lands to Complete Land Exchanges, Forest Service, Agriculture",
      "count": 3,
      "children": [
        {
          "id": "012-2018/2021-5216-000",
          "ancestors": [
            "012",
            "012-5216"
          ],
          "description": "Acquisition of Lands to Complete Land Exchanges, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-X-5216-000",
          "ancestors": [
            "012",
            "012-5216"
          ],
          "description": "Acquisition of Lands to Complete Land Exchanges, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-2017/2020-5216-000",
          "ancestors": [
            "012",
            "012-5216"
          ],
          "description": "Acquisition of Lands to Complete Land Exchanges, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5215",
      "ancestors": [
        "012"
      ],
      "description": "Restoration of Forest Lands and Improvements, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5215-000",
          "ancestors": [
            "012",
            "012-5215"
          ],
          "description": "Restoration of Forest Lands and Improvements, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5214",
      "ancestors": [
        "012"
      ],
      "description": "Licensee Programs, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5214-000",
          "ancestors": [
            "012",
            "012-5214"
          ],
          "description": "Licensee Programs, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5213",
      "ancestors": [
        "012"
      ],
      "description": "Payment to Minnesota (Cook, Lake, and Saint Louis Counties) from the National Forests Fund, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5213-000",
          "ancestors": [
            "012",
            "012-5213"
          ],
          "description": "Payment to Minnesota (Cook, Lake, and Saint Louis Counties) from the National Forests Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5209",
      "ancestors": [
        "012"
      ],
      "description": "Funds for Strengthening Markets, Income, and Supply (Section 32), Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5209-000",
          "ancestors": [
            "012",
            "012-5209"
          ],
          "description": "Funds for Strengthening Markets, Income, and Supply (Section 32), Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5208",
      "ancestors": [
        "012"
      ],
      "description": "Acquisition of Lands for National Forests, Special Acts, Forest Service, Agriculture",
      "count": 3,
      "children": [
        {
          "id": "012-2019/2019-5208-000",
          "ancestors": [
            "012",
            "012-5208"
          ],
          "description": "Acquisition of Lands for National Forests, Special Acts, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-2018/2018-5208-000",
          "ancestors": [
            "012",
            "012-5208"
          ],
          "description": "Acquisition of Lands for National Forests, Special Acts, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-2017/2017-5208-000",
          "ancestors": [
            "012",
            "012-5208"
          ],
          "description": "Acquisition of Lands for National Forests, Special Acts, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5207",
      "ancestors": [
        "012"
      ],
      "description": "Range Betterment Fund, Forest Service, Agriculture",
      "count": 4,
      "children": [
        {
          "id": "012-2019/2022-5207-000",
          "ancestors": [
            "012",
            "012-5207"
          ],
          "description": "Range Betterment Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-2018/2021-5207-000",
          "ancestors": [
            "012",
            "012-5207"
          ],
          "description": "Range Betterment Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-X-5207-000",
          "ancestors": [
            "012",
            "012-5207"
          ],
          "description": "Range Betterment Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        },
        {
          "id": "012-2017/2020-5207-000",
          "ancestors": [
            "012",
            "012-5207"
          ],
          "description": "Range Betterment Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5206",
      "ancestors": [
        "012"
      ],
      "description": "Expenses, Brush Disposal, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5206-000",
          "ancestors": [
            "012",
            "012-5206"
          ],
          "description": "Expenses, Brush Disposal, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5205",
      "ancestors": [
        "012"
      ],
      "description": "Native American Institutions Endowment Fund, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5205-000",
          "ancestors": [
            "012",
            "012-5205"
          ],
          "description": "Native American Institutions Endowment Fund, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5204",
      "ancestors": [
        "012"
      ],
      "description": "Timber Salvage Sales, Forest Service,  Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5204-000",
          "ancestors": [
            "012",
            "012-5204"
          ],
          "description": "Timber Salvage Sales, Forest Service,  Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5203",
      "ancestors": [
        "012"
      ],
      "description": "Roads and Trails for States, National Forests Fund, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5203-000",
          "ancestors": [
            "012",
            "012-5203"
          ],
          "description": "Roads and Trails for States, National Forests Fund, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5202",
      "ancestors": [
        "012"
      ],
      "description": "Timber Roads, Purchaser Elections, Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5202-000",
          "ancestors": [
            "012",
            "012-5202"
          ],
          "description": "Timber Roads, Purchaser Elections, Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5201",
      "ancestors": [
        "012"
      ],
      "description": "Payments to States, National Forests Fund,  Forest Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5201-000",
          "ancestors": [
            "012",
            "012-5201"
          ],
          "description": "Payments to States, National Forests Fund,  Forest Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    },
    {
      "id": "012-5161",
      "ancestors": [
        "012"
      ],
      "description": "Agricultural Quarantine Inspection User Fee Account, Animal and Plant Health Inspection Service, Agriculture",
      "count": 1,
      "children": [
        {
          "id": "012-X-5161-000",
          "ancestors": [
            "012",
            "012-5161"
          ],
          "description": "Agricultural Quarantine Inspection User Fee Account, Animal and Plant Health Inspection Service, Agriculture",
          "count": 0,
          "children": null
        }
      ]
    }
  ]
}
```

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
    + Body
```
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
```


## Data Structures

### TASFilterTreeNode (object)

+ `id` (required, string)
+ `description` (required, string)
+ `ancestors` (required, array[string])
+ `count` (required, number)
+ `children` (required, array[TASFilterTreeNode], nullable)