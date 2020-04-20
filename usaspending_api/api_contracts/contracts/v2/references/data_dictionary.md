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
                            "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                            "1862_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "Contracts",
                            "is1862landgrantcollege",
                            null
                        ],
                        [
                            "1890 Land Grant College",
                            "https://www.sam.gov",
                            "1890 Land Grant College",
                            "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                            "1890_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "Contracts",
                            "is1890landgrantcollege",
                            null
                        ],
                        [
                            "1994 Land Grant College",
                            "https://www.sam.gov",
                            "1994 Land Grant College",
                            "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                            "1994_land_grant_college",
                            null,
                            null,
                            null,
                            null,
                            "Contracts",
                            "is1994landgrantcollege",
                            null
                        ],
                        [
                            "8a Program Participant",
                            "List characteristic of the contractor such as whether the selected contractor is an 8(a) Program Participant Organization or not. It can be derived from the SAM data element, 'Business Types'.",
                            "8(a) Program Participant",
                            "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                            "c8a_program_participant",
                            null,
                            null,
                            null,
                            null,
                            "Contracts",
                            "firm8aflag",
                            null
                        ],
                        [
                            "A-76 FAIR Act Action",
                            "Indicates whether the contract action resulted from an A- 76/Fair Act competitive sourcing process.",
                            "A-76 (FAIR Act) Action",
                            "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                            "a76_fair_act_action_code",
                            null,
                            null,
                            null,
                            null,
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
                            "raw": "fpds_element",
                            "display": "FPDS Data Dictionary Element"
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
                        "total_rows": 412,
                        "total_size": "57.03KB",
                        "total_columns": 12,
                        "download_location": "https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx"
                    },
                    "sections": [
                        {
                            "colspan": 3,
                            "section": "Schema Data Label & Description"
                        },
                        {
                            "colspan": 6,
                            "section": "USA Spending Downloads"
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
