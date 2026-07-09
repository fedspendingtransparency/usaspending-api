FORMAT: 1A
HOST: https://api.usaspending.gov

# Data Dictionary [/api/v2/references/data_dictionary/]

This endpoint powers USAspending.gov's data dictionary page.

## GET

This endpoint returns data corresponding to the latest data dictionary csv file.

+ Response 200 (application/json)
    + Attributes (object)
        + `document` (object)
            + `metadata` (required, DictionaryMetadata)
            + `sections` (array[Section], fixed-type)
            + `headers` (array[Column], fixed-type)
            + `rows` (required, array[Row], fixed-type)

    + Body

            {
                "document": {
                    "rows": [
                        [
                            "1862 Land Grant College",
                            "https://www.sam.gov",
                            "1862 Land Grant College",
                            "Award Recipient",
                            "F = False\nT = True",
                            "N/A",
                            "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                            "1862_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "transaction_fpds",
                            "c1862_land_grant_college",
                            "Contracts",
                            "is1862landgrantcollege",
                            null
                        ],
                        [
                            "1890 Land Grant College",
                            "https://www.sam.gov",
                            "1890 Land Grant College",
                            "Award Recipient",
                            "F = False\nT = True",
                            "N/A",
                            "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                            "1890_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "transaction_fpds",
                            "c1890_land_grant_college",
                            "Contracts",
                            "is1890landgrantcollege",
                            null
                        ],
                        [
                            "1994 Land Grant College",
                            "https://www.sam.gov",
                            "1994 Land Grant College",
                            "Award Recipient",
                            "F = False\nT = True",
                            "N/A",
                            "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                            "1994_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "transaction_fpds",
                            "c1994_land_grant_college",
                            "Contracts",
                            "is1994landgrantcollege",
                            null
                        ],
                        [
                            "8a Program Participant",
                            "List characteristic of the contractor such as whether the selected contractor is an 8(a) Program Participant Organization or not. It can be derived from the SAM data element, 'Business Types'.",
                            "SBA-Certified 8(a) Program Participant",
                            "Award Recipient",
                            "F = False\nT = True",
                            "N/A",
                            "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                            "c8a_program_participant",
                            null,
                            null,
                            null,
                            null,
                            "transaction_fpds",
                            "c8a_program_participant",
                            "Contracts",
                            "firm8aflag",
                            null
                        ],
                        [
                            "A-76 FAIR Act Action",
                            "Indicates whether the contract action has resulted from an A-76/Fair Act competitive sourcing process.",
                            "A-76 (FAIR Act) Action",
                            "Award Attribute",
                            "F = False\nT = True",
                            "Y = Contract action resulted from an A-76/Fair Act competitive sourcing process.\nN = Contract action did not result from an A-76/Fair Act competitive sourcing process.",
                            "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                            "a76_fair_act_action_code",
                            null,
                            null,
                            null,
                            null,
                            "transaction_fpds",
                            "a_76_fair_act_action",
                            "Contracts",
                            "a76action",
                            null
                        ]
                    ],
                    "headers": [
                        {
                            "raw": "element",
                            "display": "Element"
                        },
                        {
                            "raw": "definition",
                            "display": "Definition"
                        },
                        {
                            "raw": "fpds_data_dictionary_element",
                            "display": "FPDS Data Dictionary Element"
                        },
                        {
                            "raw": "grouping",
                            "display": "Grouping"
                        },
                        {
                            "raw": "domain_values",
                            "display": "Domain Values"
                        },
                        {
                            "raw": "domain_values_code_description",
                            "display": "Domain Values Code Description"
                        },
                        {
                            "raw": "award_file",
                            "display": "Award File"
                        },
                        {
                            "raw": "award_element",
                            "display": "Award Element"
                        },
                        {
                            "raw": "subaward_file",
                            "display": "Subaward File"
                        },
                        {
                            "raw": "subaward_element",
                            "display": "Subaward Element"
                        },
                        {
                            "raw": "account_file",
                            "display": "Account File"
                        },
                        {
                            "raw": "account_element",
                            "display": "Account Element"
                        },
                        {
                            "raw": "table",
                            "display": "Table"
                        },
                        {
                            "raw": "database_element",
                            "display": "Element"
                        },
                        {
                            "raw": "legacy_award_file",
                            "display": "Award File"
                        },
                        {
                            "raw": "legacy_award_element",
                            "display": "Award Element"
                        },
                        {
                            "raw": "legacy_subaward_element",
                            "display": "Subaward Element"
                        }
                    ],
                    "metadata": {
                        "total_rows": 461,
                        "total_size": "110.90KB",
                        "total_columns": 17,
                        "download_location": "https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx"
                    },
                    "sections": [
                        {
                            "colspan": 6,
                            "section": "Schema Data Label & Description"
                        },
                        {
                            "colspan": 6,
                            "section": "USA Spending Downloads"
                        },
                        {
                            "colspan": 2,
                            "section": "Database Download"
                        },
                        {
                            "colspan": 3,
                            "section": "Legacy USA Spending"
                        }
                    ]
                }
            }

# Data Structures

## Section (object)
+ `section` (required, string)
+ `colspan` (required, number)
    The number of columns in the section

## Column (object)
+ `raw` (required, string)
+ `display` (required, string)

## Row (array[string], nullable)

## DictionaryMetadata (object)
+ `total_rows` (required, number)
+ `total_columns` (required, number)
+ `total_size` (required, string)
+ `download_location` (required, string)
