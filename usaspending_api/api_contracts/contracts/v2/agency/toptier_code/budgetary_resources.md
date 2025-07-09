FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/agency/{toptier_code}/budgetary_resources/]

Returns budgetary resources and obligations for the agency and fiscal year requested.

## GET

+ Parameters
    + `toptier_code`: 075 (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `agency_data_by_year` (required, AgencyData, fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "075",
                "agency_by_year": [
                    {
                        "fiscal_year": 2021,
                        "agency_budgetary_resources": 2312064788963.38,
                        "agency_total_obligated": 1330370762556.73,
                        "agency_total_outlayed": 981,694,026,406.65
                        "total_budgetary_resources": 2726162666269.23,
                        "agency_obligation_by_period": [
                            {
                                "period": 1,
                                "obligated": 46698411999.28
                            },
                            {
                                "period": 2,
                                "obligated": 85901744451.98
                            },
                            {
                                "period": 3,
                                "obligated": 120689245470.66
                            },
                            {
                                "period": 4,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 5,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 6,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 7,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 8,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 9,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 10,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 11,
                                "obligated": 170898908395.86
                            }
                            {
                                "period": 12,
                                "obligated": 170898908395.86
                            }
                        ]
                    },
                    {
                        "fiscal_year": 2020,
                        "agency_budgetary_resources": 14011153816723.11,
                        "agency_total_obligated": 8517467330750.3,
                        "agency_total_outlayed": 5,493,686,485,972.81
                        "total_budgetary_resources": 68664861885470.66,
                        "agency_obligation_by_period": [
                            {
                                "period": 3,
                                "obligated": 120689245470.66
                            },
                            {
                                "period": 6,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 7,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 8,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 9,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 10,
                                "obligated": 170898908395.86
                            },
                            {
                                "period": 11,
                                "obligated": 170898908395.86
                            }
                            {
                                "period": 12,
                                "obligated": 170898908395.86
                            }
                        ]
                    },
                    {
                        "fiscal_year": 2019,
                        "agency_budgetary_resources": 7639156008853.84,
                        "agency_total_obligated": 4458093517698.44,
                        "agency_total_outlayed": 3,181,062,491,155.40
                        "total_budgetary_resources": 34224736936338.08,
                        "agency_obligation_by_period": [
                            {
                                "period": 3,
                                "obligated": 46698411999.28
                            },
                            {
                                "period": 6,
                                "obligated": 85901744451.98
                            },
                            {
                                "period": 9,
                                "obligated": 120689245470.66
                            },
                            {
                                "period": 12,
                                "obligated": 170898908395.86
                            }
                        ]
                    },
                    {
                        "fiscal_year": 2018,
                        "agency_budgetary_resources": 6503160322408.84,
                        "agency_total_obligated": 4137177463626.79,
                        "agency_total_outlayed": 2,365,982,858,782.05
                        "total_budgetary_resources": 28449025364570.94,
                        "agency_obligation_by_period": [
                            {
                                "period": 3,
                                "obligated": 46698411999.28
                            },
                            {
                                "period": 6,
                                "obligated": 85901744451.98
                            },
                            {
                                "period": 9,
                                "obligated": 120689245470.66
                            },
                            {
                                "period": 12,
                                "obligated": 170898908395.86
                            }
                        ]
                    },
                    {
                        "fiscal_year": 2017,
                        "agency_budgetary_resources": 4994322260247.61,
                        "agency_total_obligated": 3668328859224.09,
                        "agency_total_outlayed": 1,325,993,401,023.52
                        "total_budgetary_resources": 18710078230235.09,
                        "agency_obligation_by_period": [
                            {
                                "period": 3,
                                "obligated": 46698411999.28
                            },
                            {
                                "period": 6,
                                "obligated": 85901744451.98
                            },
                            {
                                "period": 9,
                                "obligated": 120689245470.66
                            },
                            {
                                "period": 12,
                                "obligated": 170898908395.86
                            }
                        ]
                    }
                ],
                "messages": []
            }

# Data Structures

## ObligationAndPeriod (object)
+ `period` (required, number)
+ `obligated` (required, number)

## AgencyData (object)
+ `fiscal_year` (required, number)
+ `agency_budgetary_resources` (required, number, nullable)
    The agency's budget for the provided fiscal year
+ `total_budgetary_resources` (required, number, nullable)
    The budget for all agencies in the provided fiscal year
+ `agency_total_obligated` (required, number, nullable)
+ `agency_total_outlayed` (required, number, nullable)
+ `agency_obligation_by_period` (required, array[ObligationAndPeriod])