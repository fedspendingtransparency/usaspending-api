## Awards Last Updated
**Route:** `/api/v2/awards/last_updated/`

**Method:** `GET`

This route sends a request to the backend to retrieve the last updated date for the Award data.

### Response (JSON)

```
{
  "last_updated": "08/01/2017"
}
```

`last_updated` will be an empty string if there is no Award data available to check against.


### Errors
Possible HTTP Status Codes:
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
