import json
from decimal import Decimal

import boto3



class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


class ConnectionModel:
    """Post a payload to one live socket. The cloud endpoint stays in this class."""

    def __init__(self, endpoint_url=None, config=None):
        config = config or {}
        self.endpoint_url = endpoint_url if endpoint_url is not None else config.get("WEBSOCKET_CONNECTIONS", "")
        self._client = None

    def client(self):
        if not self.endpoint_url:
            return None
        if self._client is None:
            self._client = boto3.client(
                "apigatewaymanagementapi",
                endpoint_url=self.endpoint_url,
            )
        return self._client

    def post(self, connection_id, payload):
        """Send payload. Dicts are JSON-encoded. Strings and bytes are sent as given."""
        sender = self.client()
        if sender is None:
            raise RuntimeError("WebSocket endpoint is not configured")
        if isinstance(payload, (bytes, bytearray)):
            data = payload
        elif isinstance(payload, str):
            data = payload
        else:
            data = json.dumps(payload, cls=DecimalEncoder)
        return sender.post_to_connection(ConnectionId=connection_id, Data=data)
