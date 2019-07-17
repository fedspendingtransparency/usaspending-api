import json

from rest_framework import status


def check_autocomplete(route, client, fields, value, expected):
    """Shared internals for autocomplete tests.

    Don't use independently; call from a test function
    """
    for match_objs in (0, 1):
        resp = client.post(
            "/api/v1/{}/autocomplete/".format(route),
            content_type="application/json",
            data=json.dumps({"fields": fields, "value": value, "matched_objects": match_objs}),
        )
        assert resp.status_code == status.HTTP_200_OK

        # TODO: make this a truly v2 endpoint or change frontend to accept 'matched_awards' as top level key
        # This is for the v1 awards autocomplete
        try:
            results = resp.data["results"]
        except KeyError:
            continue

        for key in results:
            sorted(results[key]) == expected[key]

        # If matched_objects requested, verify they are present
        assert ("matched_objects" in resp.data) == match_objs
        if match_objs:
            objs = resp.data["matched_objects"]
            for field in fields:
                assert field in objs
                # and return correct number of objects
                assert len(objs[field]) == len(results[field])
