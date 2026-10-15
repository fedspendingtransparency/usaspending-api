from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return {"message": "Hello from the DABS Lambda!", "received_event": event}
