## Federal Account Spending Over Time
**Route:** `/api/v2/federal_accounts/<PK>/spending_over_time/`

**Method:** `POST`

This route takes a federal_account DB ID and returns the data reqired to visualized the spending over time graphic.


### Request

```
{
	"group": "fiscal_year"
	"filters": {
	    "object_class": [
	    {
	      "major_object_class": 10,  # Personnel compensation and benefits
	      "object_class": [111, 113, ...]  # Full-time permanent, Other than full-time permanent, ...
	    },
	    {
	      "major_object_class": 90  # Other

	    }
	    ],
	    "program_activiy": [1, 2, 3],
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
		{
		'time_period': {'fy': '2017', 'quarter': '3'},
		"outlay":0,
		"obligations_incurred_filtered": 0,
		"obligations_incurred_other": 0,
		"unobliged_balance": 0
		},...
	]
    }
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
