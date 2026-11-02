from typing import Any, Dict, Optional

import boto3
from renglo.config_names import setting


class RagModel:
    """Bedrock Agent runtime and control-plane calls."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        bedrock_client: Optional[Any] = None,
    ):
        self.config = config or {}
        self.region = region_name or setting(self.config, "REGION", "us-east-1")
        self.client = bedrock_client or boto3.client(
            "bedrock-agent-runtime",
            region_name=self.region,
        )
        self._agent = None

    def retrieve(self, **params):
        return self.client.retrieve(**params)

    def retrieve_and_generate(self, **params):
        return self.client.retrieve_and_generate(**params)

    def start_ingestion_job(self, **params):
        return self._agent_client().start_ingestion_job(**params)

    def get_ingestion_job(self, **params):
        return self._agent_client().get_ingestion_job(**params)

    def _agent_client(self):
        if self._agent is None:
            self._agent = boto3.client("bedrock-agent", region_name=self.region)
        return self._agent
