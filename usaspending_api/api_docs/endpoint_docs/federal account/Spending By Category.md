## Federal Account Spending By Category
**Route:** `/api/v2/federal_accounts/<PK>/spending_by_category/`

**Method:** `POST`

This route takes a federal_account DB ID and returns the data reqired to visualized the Spending By Category graphic.

### Request

```
{
	"category": "program_activity"
	"filters": {
	    "object_classes": [
	    {
	      "major_object_class_name": "Personnel compensation and benefits",
	      "object_class_names": ["Full-time permanent", "Other than full-time permanent", ...]
	    },
	    {
	      "major_object_class_name": "Other"
	    }
	    ],
	    "program_activites": [1, 2, 3],
	    "time_period": [
		{
			"start_date": "2001-01-01",
			"end_date": "2001-01-31"
		}
	    ]
	}
}
```


  
### Response (JSON)

```
{
    "results": 
        [
	"Program Activity 1": 110,
	"Program Activity 2": 10, 
	"Program Activity 3": 0
	]
    
```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
