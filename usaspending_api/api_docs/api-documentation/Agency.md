## Agency
**Route:** `v2/references/agency/[agency_id]/`

**Method:** `GET`

This route sends a request to the backend to retrieve Agency Data.

### Response (JSON) 

```
{
    "results": {
        "agency_name": "Department of Education",
        "active_fy": "2017",
        "active_fq": "4",
        "outlay_amount": "132552040033.37",
        "obligated_amount": "139921871902.06",
        "budget_authority_amount": "152186993659.59",
        "current_total_budget_authority_amount": "8361447130497.72",
        "mission": "ED's mission is to promote student achievement and preparation for global competitiveness by fostering educational excellence and ensuring equal access.",
        "website": "https://www.ed.gov/",
        "icon_filename": "DOE.jpg"
    }
}

```

### Errors
Possible HTTP Status Codes:
* 200 with response: {"results": { }}
      (agency id does not exist or agency has not made a submission)
* 500 : All other errors
      ```
      {
        "detail": "Sample error message"
      }
      ```
