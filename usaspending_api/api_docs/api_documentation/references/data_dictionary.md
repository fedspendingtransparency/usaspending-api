## [Data Dictionary](#data_dictionary)
**Route:** `/api/v2/references/data_dictionary/`

**Method:** `GET`

This route takes no parameters and returns a JSON structure of the Schema team's Rosetta Crosswalk Data Dictionary

### Request
*Not Applicable*

### Response (JSON)

```
{
   "rows":[
      [
         "1862 Land Grant College",
         "https://www.sam.gov",
         "1862 Land Grant College",
         "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
         "1862_land_grant_college",
         "",
         "",
         "",
         "",
         "Contracts",
         "is1862landgrantcollege",
         ""
      ]
   ],
   "headers":[
      {
         "raw":"element",
         "display":"Element"
      },
      {
         "raw":"definition",
         "display":"Definition"
      },
      {
         "raw":"fpds_element",
         "display":"FPDS Data Dictionary Element"
      },
      {
         "raw":"award_file",
         "display":"Award File"
      },
      {
         "raw":"award_element",
         "display":"Award Element"
      },
      {
         "raw":"subaward_file",
         "display":"Subaward File"
      },
      {
         "raw":"subaward_element",
         "display":"Subaward Element"
      },
      {
         "raw":"account_file",
         "display":"Account File"
      },
      {
         "raw":"account_element",
         "display":"Account Element"
      },
      {
         "raw":"legacy_award_file",
         "display":"Award File"
      },
      {
         "raw":"legacy_award_element",
         "display":"Award Element"
      },
      {
         "raw":"legacy_subaward_element",
         "display":"Subaward Element"
      }
   ],
   "metadata":{
      "total_rows":1,
      "total_size":"10.80KB",
      "total_columns":12,
      "download_location":""
   },
   "sections":[
      {
         "colspan":3,
         "section":"Schema Data Label & Description"
      },
      {
         "colspan":6,
         "section":"USA Spending Downloads"
      },
      {
         "colspan":3,
         "section":"Legacy USA Spending"
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
