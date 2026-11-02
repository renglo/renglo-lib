"""
WebSocket client wrapper that supports both AWS API Gateway and local dev WebSocket service.
"""
import json
import requests
from typing import Any
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


class WebSocketClient:
    """
    WebSocket client that abstracts away the differences between:
    - AWS API Gateway WebSocket Management API (production)
    - Local dev WebSocket service (development)
    """
    
    def __init__(self, websocket_url: str):
        """
        Initialize WebSocket client.
        
        Args:
            websocket_url: WebSocket connection URL. If it contains 'localhost' or '127.0.0.1',
                          it will use the local dev service, otherwise AWS API Gateway.
        """
        self.websocket_url = websocket_url
        self.is_local = False
        self.local_ws_url = None
        
        if not websocket_url:
            return
        
        # Check if using local dev WebSocket service
        if 'localhost' in websocket_url or '127.0.0.1' in websocket_url or '0.0.0.0' in websocket_url:
            print('Initializing local Websocket service')
            self.is_local = True
            # Convert ws:// to http:// for local service
            self.local_ws_url = websocket_url.replace('ws://', 'http://').replace('wss://', 'https://')
            if not self.local_ws_url.startswith('http'):
                self.local_ws_url = f"http://{self.local_ws_url}"
            # Remove trailing slash and /ws if present
            self.local_ws_url = self.local_ws_url.rstrip('/').replace('/ws', '')
        else:
            print('Initializing AWS API Gateway Websocket service')
            self.is_local = False
    
    def is_configured(self) -> bool:
        """Check if WebSocket client is properly configured."""
        if self.is_local:
            return self.local_ws_url is not None
        return bool(self.websocket_url)
    
    def send_message(self, connection_id: str, payload: Any) -> bool:
        """
        Send a message to a WebSocket connection.
        
        Args:
            connection_id: The WebSocket connection ID
            payload: The message payload to send
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not connection_id or not self.is_configured():
            return False
        
        try:
            if self.is_local:
                # Local dev WebSocket service - use HTTP POST
                #print(f"Posting locally to: {self.local_ws_url}")
                response = requests.post(
                    f"{self.local_ws_url}",
                    json={
                        "connection_id": connection_id,
                        "payload": payload
                    },
                    timeout=5
                )
                response.raise_for_status()
                result = response.json()
                if not result.get('ok', False):
                    print(f'Error sending message to local WebSocket: {result.get("error", "Unknown error")}')
                    return False
            else:
                # Cloud socket. Strings and bytes are posted unchanged.
                from renglo.chat.connection_model import ConnectionModel

                print(f"Posting to AWS WSS")
                ConnectionModel(endpoint_url=self.websocket_url).post(connection_id, payload)
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f'Error sending message to local WebSocket: {str(e)}')
            return False
        except Exception as e:
            # Handle AWS API Gateway exceptions
            if not self.is_local:
                # Check if it's a GoneException (connection closed)
                exception_name = type(e).__name__
                if exception_name == 'GoneException':
                    print(f'Connection is no longer available')
                    return False
            print(f'Error sending message: {str(e)}')
            return False

