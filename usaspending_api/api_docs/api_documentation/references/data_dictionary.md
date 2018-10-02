## [Data Dictionary](#data_dictionary)
**Route:** `/api/v2/references/data_dictionary/`

**Method:** `GET`

This route takes no parameters and returns a JSON structure of the Schema team's Rosetta Crosswalk Data Dictionary

### Request
*Not Applicable*

### Response (JSON)

```
{
  "rows": [
    [
      "Interstate Entity",
      "https://www.sam.gov",
      "Interstate Entity",
      "D1",
      "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
      "interstate_entity",
      null,
      null,
      null,
      null,
      "isinterstateentity",
      null
    ],
    [
      "Joint Venture Economically Disadvantaged Women Owned Small Business",
      "https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is an Economically Disadvantaged Woman Owned Small Business or not. It can be derived from the SAM data element, 'Business Types'.",
      "Joint Venture Economically Disadvantaged Women Owned Small Business",
      "D1",
      "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
      "joint_venture_economic_disadvantaged_women_owned_small_bus",
      null,
      null,
      null,
      null,
      "isjointventureecondisadvwomenownedsmallbusiness",
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
      "display": "FPDS Element"
    },
    {
      "raw": "file_a_f",
      "display": "File\nA-F"
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
      "raw": "legacy_award_element",
      "display": "Award Element"
    },
    {
      "raw": "legacy_subaward_element",
      "display": "Subaward Element"
    }
  ],
  "metadata": {
    "file_name": "file.xlsx",
    "total_rows": 393,
    "total_size": "119.32KB",
    "total_columns": 12
  },
  "sections": [
    {
      "colspan": 3,
      "section": "Schema Data Label & Description"
    },
    {
      "colspan": 1,
      "section": "File"
    },
    {
      "colspan": 6,
      "section": "USA Spending Downloads"
    },
    {
      "colspan": 2,
      "section": "Legacy USA Spending"
    }
  ]
}

```


### Errors
Possible HTTP Status Codes:
* 200 : Success
* 204 : No data found in database
* 500 : All other errors

```
{
    "detail": "Sample error message"
}
```
