import dredd_hooks as hooks
import json
import requests


def _post_request_response(protocol, host, port, path, body):
    url_components = {
        "protocol": protocol,
        "host": host,
        "port": ":" + port if port else "",
        "path": path
    }
    request_url = "{protocol}//{host}{port}{path}".format(**url_components)
    request_headers = {"Content-type": "application/json"}
    request_body = json.dumps(body)

    with requests.post(request_url, headers=request_headers, data=request_body) as response:
        response_body = response.json()

    return response_body


@hooks.before("./contracts/v2/bulk_download/status.md > Bulk Download Status > GET")
def before_bulk_download_status_test(transaction):
    # Run /api/v2/bulk_download/awards/ to get file_name
    body = {
        "filters": {
            "agency": 50,
            "award_types": ["contracts", "grants"],
            "date_range": {
                "start_date": "2019-01-01",
                "end_date": "2019-12-31"
            },
            "date_type": "action_date"
        },
        "award_levels": ["prime_awards", "sub_awards"]
    }
    response = _post_request_response(
        protocol=transaction.get("protocol"),
        host=transaction.get("host"),
        port=transaction.get("port"),
        path="/api/v2/bulk_download/awards/",
        body=body
    )
    file_name = response["file_name"]

    # Set the transactions path to use the file_name
    transaction["fullPath"] = transaction["fullPath"].replace("all_prime_transactions_subawards_20191017223212534453.zip", file_name)


@hooks.before("./contracts/v2/download/status.md > Download Status > GET")
def before_download_status_test(transaction):
    # Run /api/v2/download/awards/ to get file_name
    body = {
        "filters": {
            "keywords": ["Defense"]
        }
    }
    response = _post_request_response(
        protocol=transaction.get("protocol"),
        host=transaction.get("host"),
        port=transaction.get("port"),
        path="/api/v2/download/awards/",
        body=body
    )
    file_name = response["file_name"]

    # Set the transactions path to use the file_name
    transaction["fullPath"] = transaction["fullPath"].replace("all_prime_awards_subawards_20191016202911453381.zip", file_name)
