# search_client.py - OpenSearch client wrapper with AWS SigV4 auth

from typing import Optional, Any

from renglo.logger import get_logger


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    """Parse OpenSearch endpoint URL to host and port."""
    endpoint = endpoint.strip().lower()
    if endpoint.startswith('https://'):
        endpoint = endpoint[8:]
    elif endpoint.startswith('http://'):
        endpoint = endpoint[7:]
    if '/' in endpoint:
        endpoint = endpoint.split('/')[0]
    if ':' in endpoint:
        host, port_str = endpoint.rsplit(':', 1)
        try:
            port = int(port_str)
        except ValueError:
            port = 443
    else:
        host = endpoint
        port = 443
    return host, port


def create_opensearch_client(
    endpoint: str,
    region: str = 'us-east-1',
    use_ssl: bool = True,
) -> Optional[Any]:
    """
    Create OpenSearch client with AWS SigV4 authentication.
    Returns None if opensearch-py is not installed or endpoint is invalid.
    """
    if not endpoint:
        return None

    try:
        from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
        import boto3
    except ImportError:
        get_logger().warning(
            "opensearch-py not installed. Search indexing disabled. "
            "Install with: pip install opensearch-py"
        )
        return None

    try:
        host, port = _parse_endpoint(endpoint)
        credentials = boto3.Session().get_credentials()
        service = "aoss" if "aoss" in host else "es"
        auth = AWSV4SignerAuth(credentials, region, service)

        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
            use_ssl=use_ssl,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
        )
        return client
    except Exception as e:
        get_logger().error(f"Failed to create OpenSearch client: {e}")
        return None
